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
     * Tells whether an election is currenty underway.
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
            return;
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

        // Begin the election only if current epoch is over. This message might
        // have been sent after a previous election has complete
        if (!this.isElectionUnderway & msg.id.epoch == thisReplica.getEpoch()) {
            thisReplica.beginElection();
        }
        // Sends back the ACK
        thisReplica.tellWithDelay(thisReplica.getSender(), new ElectionAckMsg(msg.id));

        System.out.printf("[R%d] Election message from %s on round %d%n",
            thisReplica.getReplicaID(),
            thisReplica.getSender().path().name(),
            msg.id.isFirstRound ? 1 : 2
        );
        //System.out.printf(
        //        "[R%d] election message received from %s with content: %s%n",
        //        thisReplica.getReplicaID(),
        //        thisReplica.getSender().path().name(),
        //        msg.participants.entrySet().stream().map(
        //                (content) ->
        //                        String.format(
        //                                "{ replicaID: %d, lastUpdate: (%d, %d) }",
        //                                content.getKey(),
        //                                content.getValue().epoch,
        //                                content.getValue().index
        //                        )
        //        ).collect(Collectors.joining(", "))
        //);

        ActorRef nextNode = thisReplica.getNextNode();
        // When a node receives the election message, and the message already
        // contains the node's ID the new leader is the node with the latest
        // update, i.e. with highest (epoch, writeIndex), and highest replicaID
        if (msg.participants.containsKey(thisReplica.getReplicaID())) {
            var mostUpdated = msg.participants
                .entrySet().stream().reduce(Utils::getNewCoordinatorIndex);
            thisReplica.setCoordinatorIndex(mostUpdated.get().getKey());
            thisReplica.setIsCoordinator(
                thisReplica.getCoordinatorIndex() == thisReplica.getReplicaID()
            );

            System.out.printf(
                    "[R%d] New coordinator found: %d%n",
                    thisReplica.getReplicaID(),
                    thisReplica.getCoordinatorIndex()
            );
            if (thisReplica.isCoordinator()) {
                event = KeyEvents.BECOME_COORDINATOR;
                this.thisReplica.schedule.register(event);

                if (this.thisReplica.schedule.crashBefore(event)) {
                    return;
                }

                // This replica is the new coordinator
                thisReplica.getContext().become(thisReplica.createCoordinator());
                // The new coordinator should start sending heartbeat messages, so
                // it sends itself a start message so that the appropriate timer is
                // set
                thisReplica.getSelf().tell(new StartMsg(), thisReplica.getSelf());
                this.sendSynchronizationMessage(msg.participants);
                //System.out.format(
                //    "[Co] %s sent synchronization message%n",
                //    thisReplica.getSelf().path().name()
                //);
                thisReplica.onCoordinatorChange(thisReplica.getEpoch() + 1);

                if (this.thisReplica.schedule.crashAfter(event)) {
                    this.thisReplica.crash(event, false);
                }
                return;
            }

            // If this replica has received this election message for the
            // second time and this replica is not the new coordinator, then the
            // election message must begin its second round.
            nextMsg = new ElectionMsg(msg);
            System.out.printf("[R%d] Election passed on second round%n", thisReplica.getReplicaID());
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
            return;
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

        thisReplica.setCoordinatorIndex(thisReplica.getReplicas().indexOf(thisReplica.getSender()));
        thisReplica.setIsCoordinator(false);
        // Since no there's a new coordinator, the time of last contact must be
        // reset
        thisReplica.resetLastContact();
        //System.out.printf(
        //        "[R%d] received synchronization message from %d " + 
        //        "together with %d missed updates: %s%n",
        //        thisReplica.getReplicaID(),
        //        thisReplica.getCoordinatorIndex(),
        //        msg.missedUpdates.size(),
        //        msg.missedUpdates.stream().map(
        //                update -> String.format(
        //                        "(%d, %d)",
        //                        update.id.epoch,
        //                        update.id.index
        //                )
        //        ).collect(Collectors.toList())
        //);

        for (var update : msg.missedUpdates) {
            thisReplica.setValue(update.value);
        }
        // Updates the last write for this replica setting it to the last value
        // in the list (it's ordered).
        if (msg.missedUpdates.size() > 0) {
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
            return;
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
            return;
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
     * If this message is received while the election protocol is active, it
     * means that somewhere the election got stuck, so it must be restarted
     * again.
     */
    public void onStuckedElectionMsg(StuckedElectionMsg msg) {
        if (msg.epoch != thisReplica.getEpoch()) {
            // The election protocol is active, but for a different election.
            // The one being checked here must have completed
            return;
        }
        this.thisReplica.beginElection();
        this.sendElectionMessage();

        System.out.printf(
            "[R%d] Detected stucked election, initiating a new one%n",
            thisReplica.getReplicaID()
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
