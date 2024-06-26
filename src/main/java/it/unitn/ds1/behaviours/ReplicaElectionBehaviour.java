package it.unitn.ds1.behaviours;

import akka.actor.ActorRef;
import it.unitn.ds1.Replica;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.UpdateRequestOkMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.crash_detection.CoordinatorAckReceivedMsg;
import it.unitn.ds1.models.crash_detection.ElectionAckReceivedMsg;
import it.unitn.ds1.models.election.*;
import it.unitn.ds1.models.update.WriteMsg;
import it.unitn.ds1.utils.Delays;
import it.unitn.ds1.utils.Utils;
import it.unitn.ds1.utils.WriteId;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReplicaElectionBehaviour {
    private final List<UpdateRequestMsg> queuedUpdates = new ArrayList<>(); // Queued updates sent when the election was underway
    private final Replica thisReplica; // The replica to which this behaviour belongs
    private boolean isElectionUnderway = false; // Whether an election is currently underway
    /**
     * For each replica keeps track of its last applied write. ReplicaIDs are
     * used as keys and values WriteIds.
     */
    private Map<Integer, WriteId> lastWriteForReplica = new HashMap<>();
    /**
     * Each election is identified by an index. This is necessary for ACKs.
     */
    private int electionIndex = 0;
    /**
     * Every ElectionMsg sent must be ACKed. Pairs (sender, index) of the ACK
     * are stored and later checked.
     */
    private final Set<Map.Entry<ActorRef, Integer>> pendingElectionAcks = new HashSet<>();
    /**
     * Every CoordinatorMsg sent must be ACKed. Pairs (sender, index) of the ACK
     * are stored and later checked.
     */
    private final Set<Map.Entry<ActorRef, Integer>> pendingCoordinatorAcks = new HashSet<>();

    public ReplicaElectionBehaviour(Replica thisReplica) {
        this.thisReplica = thisReplica;
    }

    public void onUpdateRequestMsg(UpdateRequestMsg msg) {
        queuedUpdates.add(msg);
        thisReplica.tellWithDelay(
                msg.id.client,
                new UpdateRequestOkMsg(msg.id.index)
        );
    }

    public List<UpdateRequestMsg> getQueuedUpdates() {
        return queuedUpdates;
    }

    /**
     * When an ElectionMsg is received:
     * - If the message already contains this replicaID, then change the type to
     * Coordinator, and set the coordinatorID to the node which is the most
     * updated in the list (highest epoch and writeIndex), and take the node with
     * the highest ID in case of a tie;
     * - Otherwise, add the replicaID of this node + the last update to the list,
     * then propagate to the next node;
     */
    public void onElectionMsg(ElectionMsg msg) {
        if (!this.isElectionUnderway) {
            thisReplica.beginElection();
            this.isElectionUnderway = true;
            if (msg.index > this.electionIndex) {
                this.electionIndex = msg.index;
            }
        }

        // Sends back the ACK
        thisReplica.tellWithDelay(thisReplica.getSender(), new ElectionAckMsg(msg.index));

        System.out.printf(
                "[R: %d] election message received from replica %s with content: %s\n",
                thisReplica.getReplicaID(),
                thisReplica.getSender().path().name(),
                msg.participants.entrySet().stream().map(
                        (content) ->
                                String.format(
                                        "{ replicaID: %d, lastUpdate: (%d, %d) }",
                                        content.getKey(),
                                        content.getValue().epoch,
                                        content.getValue().index
                                )
                ).collect(Collectors.joining(", "))
        );

        ActorRef nextNode = thisReplica.getNextNode();
        // When a node receives the election message, and the message already
        // contains the node's ID, then change the message type to COORDINATOR.
        // The new leader is the node with the latest update
        // highest (epoch, writeIndex), and highest replicaID.
        if (msg.participants.containsKey(thisReplica.getReplicaID())) {
            var mostUpdated = msg.participants.entrySet().stream().reduce(Utils::getNewCoordinatorIndex);
            thisReplica.setCoordinatorIndex(mostUpdated.get().getKey());

            System.out.printf(
                    "[R: %d] New coordinator found: %d\n",
                    thisReplica.getReplicaID(),
                    thisReplica.getCoordinatorIndex()
            );
            if (thisReplica.getCoordinatorIndex() == thisReplica.getReplicaID()) {
                lastWriteForReplica = msg.participants;
                this.sendSynchronizationMessage();
                sendLostUpdates();
                thisReplica.onCoordinatorChange(thisReplica.getEpoch() + 1);
                return;
            }

            CoordinatorMsg coordinatorMsg = new CoordinatorMsg(
                    msg.index,
                    thisReplica.getCoordinatorIndex(),
                    thisReplica.getReplicaID(),
                    msg.participants
            );
            this.pendingElectionAcks.add(new AbstractMap.SimpleEntry<>(nextNode, msg.index));
            thisReplica.tellWithDelay(nextNode, coordinatorMsg);
            thisReplica.getContext().system().scheduler().scheduleOnce(
                    Duration.create(Delays.COORDINATOR_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                    thisReplica.getSelf(),
                    new CoordinatorAckReceivedMsg(coordinatorMsg),
                    thisReplica.getContext().system().dispatcher(),
                    nextNode
            );
            return;
        }
        // If it's an election message, and my ID is not in the list, add it and
        // propagate to the next node.
        msg.participants.put(thisReplica.getReplicaID(), thisReplica.getLastWrite());
        thisReplica.tellWithDelay(nextNode, msg);
        this.pendingElectionAcks.add(new AbstractMap.SimpleEntry<>(nextNode, msg.index));
        thisReplica.getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                thisReplica.getSelf(),
                new ElectionAckReceivedMsg(msg),
                thisReplica.getContext().system().dispatcher(),
                nextNode
        );
    }

    /**
     * The Election message has been received by all the nodes, and the
     * coordinator has been elected. The coordinator sends a message to all the
     * nodes to synchronize the epoch and the writeIndex.
     */
    public void onCoordinatorMsg(CoordinatorMsg msg) {
        thisReplica.tellWithDelay(thisReplica.getSender(), new CoordinatorAckMsg(msg.index));

        // The replica is the sender of the message, so it already has the
        // coordinator index
        if (msg.senderID == thisReplica.getReplicaID())
            return;
        // This replica is the new coordinator
        if (msg.coordinatorID == thisReplica.getReplicaID()) {
            lastWriteForReplica = msg.participants;
            this.sendSynchronizationMessage();
            sendLostUpdates();
            return;
        }
        System.out.printf(
                "[R%d] received new coordinator %d from %d%n",
                thisReplica.getReplicaID(), msg.coordinatorID, msg.senderID
        );
        thisReplica.setCoordinatorIndex(msg.coordinatorID); // Set the new coordinator

        ActorRef nextNode = thisReplica.getNextNode();
        // Forward the message to the next node
        thisReplica.tellWithDelay(nextNode, msg);

        this.pendingCoordinatorAcks.add(new AbstractMap.SimpleEntry<>(nextNode, msg.index));
        thisReplica.getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.COORDINATOR_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                thisReplica.getSelf(),
                new CoordinatorAckReceivedMsg(msg),
                thisReplica.getContext().system().dispatcher(),
                nextNode
        );
    }

    /**
     * The new coordinator has sent the synchronization message, so the replicas
     * can update their epoch and writeIndex.
     */
    public void onSynchronizationMsg(SynchronizationMsg msg) {
        thisReplica.setCoordinatorIndex(thisReplica.getReplicas().indexOf(thisReplica.getSender()));
        var isCoordinator = thisReplica.getReplicaID() == thisReplica.getCoordinatorIndex();
        thisReplica.setIsCoordinator(isCoordinator);
        thisReplica.onCoordinatorChange(msg.epoch);
        if (isCoordinator) { // Multicast sends to itself
            thisReplica.getContext().become(thisReplica.createCoordinator());
            // The new coordinator should start sending heartbeat messages, so
            // it sends itself a start message so that the appropriate timer is
            // set
            thisReplica.getSelf().tell(new StartMsg(), thisReplica.getSelf());
            return;
        }
        // Since no there's a new coordinator, the time of last contact must be
        // reset
        thisReplica.resetLastContact();
        // Exit the election state and go back to normal
        thisReplica.getContext().become(thisReplica.createReceive());
        System.out.printf(
                "[R%d] received synchronization message from %d\n",
                thisReplica.getReplicaID(),
                thisReplica.getCoordinatorIndex()
        );
    }

    /**
     * When receiving the ACK for the election message sent to the next node in the ring
     */
    public void onElectionAckMsg(ElectionAckMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(thisReplica.getSender(), msg.index);
        // The ACK has arrived, so remove it from the set of pending ones
        this.pendingElectionAcks.remove(pair);
    }

    public void onCoordinatorAckMsg(CoordinatorAckMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(thisReplica.getSender(), msg.index);
        // The ACK has arrived, so remove it from the set of pending ones
        this.pendingCoordinatorAcks.remove(pair);
    }

    /**
     * When the timeout for the ACK on the election message is received, if the pair is still in the map
     * it means that the replica has not received the acknowledgement, and so we add it to the crashed replicas
     */
    public void onElectionAckTimeoutReceivedMsg(ElectionAckReceivedMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(thisReplica.getSender(), msg.msg.index);
        // If the pair is still in the set, the replica who should have sent the
        // ACK is probably crashed
        if (!this.pendingElectionAcks.contains(pair)) {
            return;
        }

        thisReplica.getCrashedReplicas().add(thisReplica.getSender());
        // The election message should be sent again
        ActorRef nextNode = thisReplica.getNextNode();
        if (nextNode == thisReplica.getSelf()) {
            // There's no other active replica, so this should become the
            // coordinator
            thisReplica.getSelf().tell(new SynchronizationMsg(thisReplica.getEpoch() + 1), thisReplica.getSelf());
        } else {
            thisReplica.tellWithDelay(nextNode, msg);
        }
    }

    /**
     * When receiving the timeout for a coordinator acknowledgment, if the pair is still in the map,
     * it means that the other replica has crashed.
     */
    public void onCoordinatorAckTimeoutReceivedMsg(CoordinatorAckReceivedMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(thisReplica.getSender(), msg.msg.index);
        // If the pair is still in the set, the replica who should have sent the
        // ACK is probably crashed
        if (!this.pendingCoordinatorAcks.contains(pair)) {
            return;
        }

        // The coordinator message should be sent again
        ActorRef nextNode = thisReplica.getNextNode();
        if (nextNode == thisReplica.getSelf()) {
            // There's no other active replica, so this should become the
            // coordinator
            thisReplica.getSelf().tell(new SynchronizationMsg(thisReplica.getEpoch() + 1), thisReplica.getSelf());
        } else {
            thisReplica.tellWithDelay(nextNode, msg);
        }
    }

    /**
     * Send the synchronization message to all nodes.
     */
    public void sendSynchronizationMessage() {
        thisReplica.multicast(new SynchronizationMsg(thisReplica.getEpoch() + 1));
    }

    public void setElectionUnderway(boolean b) {
        this.isElectionUnderway = b;
    }

    /**
     * Sends an ElectionMsg to the next node.
     */
    public void sendElectionMessage() {
        var nextNode = thisReplica.getNextNode();
        var msg = new ElectionMsg(this.electionIndex, thisReplica.getReplicaID(), thisReplica.getLastWrite());
        thisReplica.tellWithDelay(nextNode, msg);
        this.electionIndex++;

        // For each election message the sender expects an ACK back
        thisReplica.getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                thisReplica.getSelf(),
                new ElectionAckReceivedMsg(msg),
                thisReplica.getContext().system().dispatcher(),
                nextNode
        );
    }

    /**
     * The coordinator, which is the one with the most recent updates, sends all
     * the missed updates to each replica.
     */
    public void sendLostUpdates() {
        for (var entry : this.lastWriteForReplica.entrySet()) {
            var replica = thisReplica.getReplicas().get(entry.getKey());
            var lastUpdate = entry.getValue();
            var missedUpdatesList = new ArrayList<WriteMsg>();
            for (int i = lastUpdate.index + 1; i < thisReplica.getLastWrite().index + 1; i++) {
                var writeId = new WriteId(lastUpdate.epoch, i);
                var ithRequest = thisReplica.getWriteRequests().get(writeId);
                missedUpdatesList.add(new WriteMsg(null, writeId, ithRequest));
            }
            if (missedUpdatesList.isEmpty())
                continue;
            thisReplica.tellWithDelay(replica, new LostUpdatesMsg(missedUpdatesList));
        }
    }

    /**
     * The replica has received the lost updates from the coordinator, so it can
     * apply them.
     */
    public void onLostUpdatesMsg(LostUpdatesMsg msg) {
        System.out.printf(
                "[R%d] received %d missed updates, last update: (%d, %d), new updates received: %s\n",
                thisReplica.getReplicaID(),
                msg.missedUpdates.size(),
                thisReplica.getLastWrite().epoch,
                thisReplica.getLastWrite().index,
                msg.missedUpdates.stream().map(
                        update -> String.format(
                                "(%d, %d)",
                                update.id.epoch,
                                update.id.index
                        )
                ).collect(Collectors.toList())
        );

        for (var update : msg.missedUpdates) {
            thisReplica.setValue(update.value);
        }

        // Exit the election state and go back to normal
        thisReplica.getContext().become(thisReplica.createReceive());
    }
}
