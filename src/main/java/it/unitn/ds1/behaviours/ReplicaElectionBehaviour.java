package it.unitn.ds1.behaviours;

import akka.actor.ActorRef;
import it.unitn.ds1.Replica;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.UpdateRequestOkMsg;
import it.unitn.ds1.models.administratives.StartMsg;
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
    /**
     * The replica to which this behaviour belongs.
     */
    private final Replica thisReplica;
    /**
     * Updates sent when the election was underway. These are put in the queue
     * in the order in which they're received.
     */
    private final List<UpdateRequestMsg> queuedUpdates = new ArrayList<>();
    /**
     * Every ElectionMsg sent must be ACKed. Pairs (sender, index) of the ACK
     * are stored and later checked.
     */
    private final Set<Map.Entry<ActorRef, Integer>> pendingElectionAcks = new HashSet<>();
    /**
     * Tells whether an election is currenty underway.
     */
    private boolean isElectionUnderway = false;
    /**
     * Each election is identified by an index. This is necessary for ACKs.
     */
    private int electionIndex = 0;

    public ReplicaElectionBehaviour(Replica thisReplica) {
        this.thisReplica = thisReplica;
    }

    //=== MESSAGE HANDLERS =====================================================
    public void onUpdateRequestMsg(UpdateRequestMsg msg) {
        queuedUpdates.add(msg);
        thisReplica.tellWithDelay(
                msg.id.client,
                new UpdateRequestOkMsg(msg.id.index)
        );
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
        // Begin the election only if current epoch is over. This message might
        // have been sent after a previous election has complete
        if (!this.isElectionUnderway & msg.endingEpoch == thisReplica.getEpoch()) {
            thisReplica.beginElection();
            if (msg.index > this.electionIndex) {
                this.electionIndex = msg.index;
            }
        }
        // Sends back the ACK
        thisReplica.tellWithDelay(thisReplica.getSender(), new ElectionAckMsg(msg.index));

        System.out.printf(
                "[R: %d] election message received from %s with content: %s\n",
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
        // The new leader is the node with the latest update, i.e. with
        // highest (epoch, writeIndex), and highest replicaID.
        if (msg.participants.containsKey(thisReplica.getReplicaID())) {
            var mostUpdated = msg.participants
                .entrySet().stream().reduce(Utils::getNewCoordinatorIndex);
            thisReplica.setCoordinatorIndex(mostUpdated.get().getKey());
            thisReplica.setIsCoordinator(
                thisReplica.getCoordinatorIndex() == thisReplica.getReplicaID()
            );

            System.out.printf(
                    "[R: %d] New coordinator found: %d\n",
                    thisReplica.getReplicaID(),
                    thisReplica.getCoordinatorIndex()
            );
            if (thisReplica.isCoordinator()) {
                //// This replica is the new coordinator
                thisReplica.getContext().become(thisReplica.createCoordinator());
                //// The new coordinator should start sending heartbeat messages, so
                //// it sends itself a start message so that the appropriate timer is
                //// set
                thisReplica.getSelf().tell(new StartMsg(), thisReplica.getSelf());
                this.sendSynchronizationMessage(msg.participants);
                thisReplica.onCoordinatorChange(thisReplica.getEpoch() + 1);
                return;
            }
        } else {
            // If it's an election message, and my ID is not in the list, add my
            // ID
            msg.participants.put(thisReplica.getReplicaID(), thisReplica.getLastWrite());
        }

        // The election message is propagated because either I've just added my
        // ID or the ID was already in the message, but I'm not the new coordinator
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
     * The new coordinator has sent the synchronization message, so replicas can
     * apply all missed updates and then enter the new epoch.
     */
    public void onSynchronizationMsg(SynchronizationMsg msg) {
        thisReplica.setCoordinatorIndex(thisReplica.getReplicas().indexOf(thisReplica.getSender()));
        thisReplica.setIsCoordinator(false);
        // Since no there's a new coordinator, the time of last contact must be
        // reset
        thisReplica.resetLastContact();
        System.out.printf(
                "[R%d] received synchronization message from %d " + 
                "together with %d missed updates: %s\n",
                thisReplica.getReplicaID(),
                thisReplica.getCoordinatorIndex(),
                msg.missedUpdates.size(),
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
            // Removes from the list of incomple updates those that where applied
            // by the new coordinator and hence have been applied now by this
            // replica
            thisReplica.getWriteRequests().remove(update.id);
        }
        // Updates the last write for this replica setting it to the last value
        // in the list (it's ordered).
        if (msg.missedUpdates.size() > 0) {
            thisReplica.setLastWrite(
                msg.missedUpdates.get(msg.missedUpdates.size() - 1).id
            );
        }
        
        // Now, every entry of writeRequests that precedes lastUpdate can be
        // discarded, since it would be ignored anyway.
        thisReplica.getWriteRequests().entrySet().removeIf(
            writeId -> writeId.getKey().isPriorOrEqualTo(thisReplica.getLastWrite())
        );
        // Any value still in the list is incomplete. However, since uniform
        // agreement requires that if any correct replica delivers, all the other
        // has to do it as well, if any replica had delivered its last update
        // would have been later than the one of the choosen coordinator. This
        // means that at this point, no replica has delivere any of the writes
        // in the list because if any did, it would have been choosen as new
        // coordinator. Since none delivered, we could just discard these writes
        // or treat them as request received in the new epoch.
        // So, now the new epoch can officially begin.
        thisReplica.getContext().become(thisReplica.createReceive());
        thisReplica.onCoordinatorChange(msg.epoch);

        System.out.format(
            "[R%d] Election complete: new epoch %d with coordinator %s\n",
            thisReplica.getReplicaID(),
            thisReplica.getEpoch(),
            thisReplica.getReplicas().get(thisReplica.getCoordinatorIndex()).path().name()
        );
    }

    //=== HANDLERS FOR CRASH DETECTION =========================================
    /**
     * When receiving the ACK for the election message sent to the next node in
     * the ring.
     */
    public void onElectionAckMsg(ElectionAckMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(thisReplica.getSender(), msg.index);
        // The ACK has arrived, so remove it from the set of pending ones
        this.pendingElectionAcks.remove(pair);
    }

    /**
     * When the timeout for the ACK on the election message is received, if the
     * pair is still in the map it means that the replica has not received the
     * acknowledgement, so we add it to the crashed replicas.
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
        thisReplica.tellWithDelay(nextNode, msg);
    }

    //=== AUXILIARIES ==========================================================
    public void setElectionUnderway(boolean b) {
        this.isElectionUnderway = b;
    }

    /**
     * Sends a new ElectionMsg to the next node. Should be used when starting
     * a new election.
     */
    public void sendElectionMessage() {
        var nextNode = thisReplica.getNextNode();
        var msg = new ElectionMsg(
            thisReplica.getEpoch(),
            this.electionIndex,
            thisReplica.getReplicaID(),
            thisReplica.getLastWrite()
        );
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
     * Send the synchronization message to all other nodes. Each messages also
     * contains the list of updated missed by the receiver.
     */
    public void sendSynchronizationMessage(Map<Integer, WriteId> lastWriteForReplica) {
        for (var entry : lastWriteForReplica.entrySet()) {
            var replica = thisReplica.getReplicas().get(entry.getKey());
            var lastUpdate = entry.getValue();
            var missedUpdatesList = new ArrayList<WriteMsg>();
            for (int i = lastUpdate.index + 1; i <= thisReplica.getLastWrite().index; i++) {
                var writeId = new WriteId(lastUpdate.epoch, i);
                var ithRequest = thisReplica.getWriteRequests().get(writeId);
                missedUpdatesList.add(new WriteMsg(null, writeId, ithRequest));
            }
            thisReplica.tellWithDelay(
                replica,
                new SynchronizationMsg(
                    thisReplica.getEpoch() + 1,
                    missedUpdatesList
                )
            );
        }
    }

    public List<UpdateRequestMsg> getQueuedUpdates() {
        return queuedUpdates;
    }
}
