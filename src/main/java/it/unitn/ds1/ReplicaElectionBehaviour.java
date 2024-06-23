package it.unitn.ds1;

import akka.actor.ActorRef;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.UpdateRequestOkMsg;
import it.unitn.ds1.models.crash_detection.*;
import it.unitn.ds1.models.election.CoordinatorMsg;
import it.unitn.ds1.models.election.ElectionAckMsg;
import it.unitn.ds1.models.election.ElectionMsg;
import it.unitn.ds1.utils.Delays;
import it.unitn.ds1.utils.Utils;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReplicaElectionBehaviour {
    private final List<UpdateRequestMsg> queuedUpdates = new ArrayList<>();
    private final Replica thisReplica;
    private boolean isElectionUnderway = false;
    private int electionIndex;

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
                thisReplica.setLastWriteForReplica(msg.participants);
                thisReplica.sendSynchronizationMessage();
                thisReplica.sendLostUpdates();
                thisReplica.onCoordinatorChange();
                return;
            }

            CoordinatorMsg coordinatorMsg = new CoordinatorMsg(
                    msg.index,
                    thisReplica.getCoordinatorIndex(),
                    thisReplica.getReplicaID(),
                    msg.participants
            );
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
        thisReplica.getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                thisReplica.getSelf(),
                new ElectionAckReceivedMsg(msg),
                thisReplica.getContext().system().dispatcher(),
                nextNode
        );
    }
}
