package it.unitn.ds1.behaviours;

import akka.actor.ActorRef;
import it.unitn.ds1.Replica;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.UpdateRequestOkMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.crash_detection.ElectionAckReceivedMsg;
import it.unitn.ds1.models.crash_detection.SynchronizationAckReceivedMsg;
import it.unitn.ds1.models.election.*;
import it.unitn.ds1.models.update.WriteMsg;
import it.unitn.ds1.utils.Delays;
import it.unitn.ds1.utils.ElectionId;
import it.unitn.ds1.utils.KeyEvents;
import it.unitn.ds1.utils.Utils;
import it.unitn.ds1.utils.WriteId;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
     * Every ElectionMsg sent must be ACKed. Pairs (sender, electionId) of the ACK
     * are stored and later checked. During each election the same replica
     * receives two election messages. These have distinct IDs and their ACK
     * as well, so they will figure as distinct elements in the set.
     */
    private final Set<Map.Entry<ActorRef, ElectionId>> pendingElectionAcks = new HashSet<>();
    /**
     * Whether the synchronization message has been received.
     */
    private boolean hasReceivedSynchronizationMessage = true;
    /**
     * Tells whether an election is currently underway.
     */
    private boolean isElectionUnderway = false;

    public ReplicaElectionBehaviour(Replica thisReplica) {
        this.thisReplica = thisReplica;
    }

    //=== MESSAGE HANDLERS =====================================================
    public void onUpdateRequestMsg(UpdateRequestMsg msg) {
        KeyEvents event = KeyEvents.UPDATE;
        this.thisReplica.schedule.register(event);

        if (this.thisReplica.schedule.crashBefore(event)) {
            this.thisReplica.crash(event, true);
            return;
        }

        queuedUpdates.add(msg);
        thisReplica.tellWithDelay(
                msg.id.client,
                new UpdateRequestOkMsg(msg.id.index)
        );

        if (this.thisReplica.schedule.crashAfter(event)) {
            this.thisReplica.crash(event, false);
        }
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
        ElectionMsg nextMsg = msg;
        KeyEvents event;
        if (!msg.participants.containsKey(thisReplica.getReplicaID())) {
            event = KeyEvents.ELECTION_1;
        } else {
            event = KeyEvents.ELECTION_2;
        }
        this.thisReplica.schedule.register(event);

        if (this.thisReplica.schedule.crashBefore(event)) {
            this.thisReplica.crash(event, true);
            return;
        }

        // Remove the crashed replicas from the participants in the election
        for (var crashedReplica : thisReplica.getCrashedReplicas()) {
            msg.participants.remove(thisReplica.getReplicas().indexOf(crashedReplica));
        }

        // Begin the election only if current epoch is over. This message might
        // have been sent after a previous election has complete
        if (!this.isElectionUnderway && msg.id.epoch == thisReplica.getEpoch()) {
            thisReplica.beginElection();
        }
        // Sends back the ACK
        thisReplica.tellWithDelay(thisReplica.getSender(), new ElectionAckMsg(msg.id));

        System.out.printf("[R%d] Election message from %s on round %d%n",
            thisReplica.getReplicaID(),
            thisReplica.getSender().path().name(),
            msg.id.isFirstRound ? 1 : 2
        );

        ActorRef nextNode = thisReplica.getNextNode();
        // When a node receives the election message, and the message already
        // contains the node's ID the new leader is the node with the latest
        // update, i.e. with highest (epoch, writeIndex), and highest replicaID
        if (msg.participants.containsKey(thisReplica.getReplicaID())) {
            nextMsg = processNewCoordinator(msg);
            // The replica has crashed
            if (nextMsg == null) {
                return;
            }
        } else {
            // If my ID is not in the list, add my ID
            msg.participants.put(thisReplica.getReplicaID(), thisReplica.getLastWrite());
        }

        // The election message is propagated because either I've just added my
        // ID or the ID was already in the message, but I'm not the new coordinator
        thisReplica.tellWithDelay(nextNode, nextMsg);
        var pair = new AbstractMap.SimpleEntry<>(nextNode, nextMsg.id);
        this.pendingElectionAcks.add(pair);
        thisReplica.getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                thisReplica.getSelf(),
                new ElectionAckReceivedMsg(nextMsg),
                thisReplica.getContext().system().dispatcher(),
                nextNode
        );

        if (this.thisReplica.schedule.crashAfter(event)) {
            this.thisReplica.crash(event, false);
        }
    }

    /**
     * When the election message has been received for the second time, we can find
     * the replica that has the requisite to become the new coordinator.
     */
    private ElectionMsg processNewCoordinator(ElectionMsg msg) {
        var mostUpdated = msg.participants
                .entrySet().stream().reduce(Utils::getNewCoordinatorIndex);

        // Couldn't find a new coordinator (this is impossible)
        if (mostUpdated.isEmpty()) {
            return null;
        }

        thisReplica.setCoordinatorIndex(mostUpdated.get().getKey());
        thisReplica.setIsCoordinator(
                thisReplica.getCoordinatorIndex() == thisReplica.getReplicaID()
        );

        this.hasReceivedSynchronizationMessage = false;
        // I'm expecting to receive the synchronization message soon.
        thisReplica.getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                thisReplica.getSelf(),
                new SynchronizationAckReceivedMsg(),
                thisReplica.getContext().system().dispatcher(),
                this.thisReplica.getSelf()
        );

        System.out.printf(
                "[R%d] New coordinator found: %d%n",
                thisReplica.getReplicaID(),
                thisReplica.getCoordinatorIndex()
        );
        if (thisReplica.isCoordinator()) {
            this.becomeCoordinator(msg);
            return null;
        }

        // If this replica has received this election message for the
        // second time and this replica is not the new coordinator, then the
        // election message must begin its second round.
        ElectionMsg nextMsg = new ElectionMsg(msg);
        System.out.printf("[R%d] Election passed on second round%n", thisReplica.getReplicaID());
        return nextMsg;
    }

    /**
     * This replica has been elected as the new coordinator. It must send the
     * synchronization message to all other replicas.
     */
    private void becomeCoordinator(ElectionMsg msg) {
        KeyEvents event = KeyEvents.BECOME_COORDINATOR;
        this.thisReplica.schedule.register(event);

        if (this.thisReplica.schedule.crashBefore(event)) {
            this.thisReplica.crash(event, true);
            return;
        }

        // This replica is the new coordinator
        thisReplica.getContext().become(thisReplica.createCoordinator());
        // The new coordinator should start sending heartbeat messages, so
        // it sends itself a start message so that the appropriate timer is
        // set
        thisReplica.getSelf().tell(new StartMsg(), thisReplica.getSelf());
        this.sendAllSynchronizationMessages(msg.participants);
        thisReplica.onCoordinatorChange(thisReplica.getEpoch() + 1);

        if (this.thisReplica.schedule.crashAfter(event)) {
            this.thisReplica.crash(event, false);
            this.thisReplica.crash(event, false);
        }
    }

    /**
     * The new coordinator has sent the synchronization message, so replicas can
     * apply all missed updates and then enter the new epoch.
     */
    public void onSynchronizationMsg(SynchronizationMsg msg) {
        KeyEvents event = KeyEvents.SYNCHRONIZATION;
        this.thisReplica.schedule.register(event);

        if (this.thisReplica.schedule.crashBefore(event)) {
            this.thisReplica.crash(event, true);
            return;
        }

        this.hasReceivedSynchronizationMessage = true;

        thisReplica.setCoordinatorIndex(thisReplica.getReplicas().indexOf(thisReplica.getSender()));
        thisReplica.setIsCoordinator(false);
        // Since no there's a new coordinator, the time of last contact must be
        // reset
        thisReplica.resetLastContact();

        for (var update : msg.missedUpdates) {
            thisReplica.setValue(update.value);
        }
        // Updates the last write for this replica setting it to the last value
        // in the list (it's ordered).
        if (!msg.missedUpdates.isEmpty()) {
            thisReplica.setLastWrite(
                msg.missedUpdates.get(msg.missedUpdates.size() - 1).id
            );
        }

        // Now the new epoch can officially begin.
        thisReplica.getContext().become(thisReplica.createReceive());
        thisReplica.onCoordinatorChange(msg.epoch);

        System.out.format(
            "[R%d] Election complete: new epoch %d with coordinator %s%n",
            thisReplica.getReplicaID(),
            thisReplica.getEpoch(),
            thisReplica.getReplicas().get(thisReplica.getCoordinatorIndex()).path().name()
        );

        if (this.thisReplica.schedule.crashAfter(event)) {
            this.thisReplica.crash(event, false);
        }
    }

    //=== HANDLERS FOR CRASH DETECTION =========================================
    /**
     * When receiving the ACK for the election message sent to the next node in
     * the ring.
     */
    public void onElectionAckMsg(ElectionAckMsg msg) {
        KeyEvents event = KeyEvents.ELECTION_ACK;
        this.thisReplica.schedule.register(event);

        if (this.thisReplica.schedule.crashBefore(event)) {
            this.thisReplica.crash(event, true);
            return;
        }

        var pair = new AbstractMap.SimpleEntry<>(thisReplica.getSender(), msg.id);
        this.pendingElectionAcks.remove(pair);

        if (this.thisReplica.schedule.crashAfter(event)) {
            this.thisReplica.crash(event, false);
        }
    }

    /**
     * When the timeout for the ACK on the election message is received, if the
     * pair is still in the map it means that the replica has not received the
     * acknowledgement, so we add it to the crashed replicas.
     */
    public void onElectionAckReceivedMsg(ElectionAckReceivedMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(thisReplica.getSender(), msg.msg.id);
        // If the pair is not in the set the ACK has arrived, so everything is
        //fine
        if (!this.pendingElectionAcks.contains(pair)) {
            return;
        }

        // If the pair is still in the set, the replica who should have sent the
        // ACK is probably crashed
        thisReplica.getCrashedReplicas().add(thisReplica.getSender());

        // The election message should be sent again
        ActorRef nextNode = thisReplica.getNextNode();
        thisReplica.tellWithDelay(nextNode, msg.msg);
        this.pendingElectionAcks.add(new AbstractMap.SimpleEntry<>(nextNode, msg.msg.id));
        thisReplica.getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                thisReplica.getSelf(),
                new ElectionAckReceivedMsg(msg.msg),
                thisReplica.getContext().system().dispatcher(),
                nextNode
        );

        System.out.printf(
            "[R%d] Detected crash of next replica [%s], trying with %s%n",
            this.thisReplica.getReplicaID(),
            this.thisReplica.getSender().path().name(),
            nextNode.path().name()
        );
    }

    /**
     * When the timeout for the ACK on the synchronization message is received, if the
     * pair is still in the map it means that the replica has not received the
     * acknowledgement, so we add it to the crashed replicas.
     */
    public void onSynchronizationAckReceivedMsg(SynchronizationAckReceivedMsg msg) {
        if (this.hasReceivedSynchronizationMessage) {
            return;
        }

        var coordinator = thisReplica.getReplicas().get(thisReplica.getCoordinatorIndex());
        thisReplica.getCrashedReplicas().add(coordinator);

        // The election message should be sent again
        sendElectionMessage();

        System.out.printf(
                "[R%d] Synchronization message not received from [%s], starting another election!\n",
                this.thisReplica.getReplicaID(),
                coordinator.path().name()
        );
    }

    //=== AUXILIARIES ==========================================================
    public void setElectionUnderway(boolean b) {
        this.isElectionUnderway = b;
    }

    /**
     * Sends a new ElectionMsg to the next node. Should be used only when
     * starting a new election.
     */
    public void sendElectionMessage() {
        var nextNode = thisReplica.getNextNode();
        var msg = new ElectionMsg(
            thisReplica.getEpoch(),
            thisReplica.getReplicaID(),
            thisReplica.getLastWrite(),
            true
        );
        thisReplica.tellWithDelay(nextNode, msg);
        this.pendingElectionAcks.add(new AbstractMap.SimpleEntry<>(nextNode, msg.id));

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
     * contains the list of updates missed by the receiver.
     */
    public void sendAllSynchronizationMessages(Map<Integer, WriteId> lastWriteForReplica) {
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
