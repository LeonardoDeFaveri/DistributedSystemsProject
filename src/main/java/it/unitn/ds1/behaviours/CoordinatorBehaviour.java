package it.unitn.ds1.behaviours;

import akka.actor.ActorRef;
import it.unitn.ds1.Replica;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.UpdateRequestOkMsg;
import it.unitn.ds1.models.update.WriteAckMsg;
import it.unitn.ds1.models.update.WriteMsg;
import it.unitn.ds1.models.update.WriteOkMsg;
import it.unitn.ds1.utils.UpdateRequestId;
import it.unitn.ds1.utils.WriteId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CoordinatorBehaviour {
    private final Replica thisReplica;
    /**
     * For each Write collects ACKs from replicas.
     */
    private final Map<WriteId, Set<ActorRef>> writeAcksMap = new HashMap<>();
    /**
     * Maps the ID of each WriteMsg to the ID of the UpdateRequestMsg that
     * caused it.
     */
    private final Map<WriteId, UpdateRequestId> writesToUpdates = new HashMap<>();
    /**
     * Index to be used for next WriteMsg created.
     */
    private int writeIndex = 0;
    /**
     * Index of the write we are currently collecting ACKs for.
     */
    private int currentWriteToAck = 0;
    /**
     * Minimum number of nodes that must agree on a Write.
     */
    private int quorum;

    public CoordinatorBehaviour(Replica thisReplica) {
        this.thisReplica = thisReplica;
    }

    //=== HANDLERS =============================================================
    public void onUpdateRequest(UpdateRequestMsg msg) {
        // If the request comes from a client, register its arrival.
        // This replica will later have to send an ACK back to this client
        if (!thisReplica.getReplicas().contains(getSender())) {
            // Immediately inform the client of the receipt of the update request
            thisReplica.tellWithDelay(getSender(), new UpdateRequestOkMsg(msg.id.index));
        }
        // The pair associated to the new writeMsg for this update request
        var writeId = new WriteId(thisReplica.getEpoch(), this.writeIndex);
        // Implement the quorum protocol. The coordinator asks all the replicas
        // to update
        thisReplica.multicast(new WriteMsg(msg.id, writeId, msg.value));

        // Associated this write to the update request that caused it
        this.writesToUpdates.putIfAbsent(writeId, msg.id);

        // Add the new write request to the map, so that the acks can be received
        this.writeAcksMap.putIfAbsent(writeId, new HashSet<>());
        this.writeIndex++;
    }

    /**
     * When the coordinator receives an ack from a replica
     */
    public void onWriteAckMsg(WriteAckMsg msg) {
        // If the epoch of the write is not the current epoch, ignore the message
        if (msg.id.epoch != thisReplica.getEpoch())
            return;

        // The OK has already been sent, as the quorum was reached.
        // ACKs from other replicas for the same write should be ignored.
        if (!this.writeAcksMap.containsKey(msg.id)) {
            return;
        }

        // Add the sender to the list
        this.writeAcksMap.get(msg.id).add(getSender());

        // Send all the messages that have been acked in FIFO order!
        sendAllAckedMessages();

        System.out.printf(
                "[Co] Received ack from %s for %d in epoch %d\n",
                getSender().path().name(),
                msg.id.index,
                msg.id.epoch
        );
    }

    //=== AUXILIARIES ==========================================================
    /**
     * The first message to be served is the currentWriteToAck index.
     * When the message is sent to the replicas, serve all the successive messages
     */
    private void sendAllAckedMessages() {
        // Starting from the first message to send, if the quorum has been reached,
        // send the message. Then, go to the next message (continue until the
        // last write has been reached).
        // If any of the writes didn't reach the quorum, stop!
        while (this.currentWriteToAck < this.writeIndex) {
            var writeId = new WriteId(thisReplica.getEpoch(), this.currentWriteToAck);
            var updateRequestId = this.writesToUpdates.get(writeId);
            if (this.writeAcksMap.containsKey(writeId) && this.writeAcksMap.get(writeId).size() >= this.quorum) {
                thisReplica.multicast(new WriteOkMsg(writeId, updateRequestId));
                this.writeAcksMap.remove(writeId);
                this.currentWriteToAck++;
            } else {
                break;
            }
        }
    }

    public void onCoordinatorChange() {
        this.writeIndex = 0;
    }

    private ActorRef getSender() {
        return thisReplica.getSender();
    }

    public void setQuorum(int quorum) {
        this.quorum = quorum;
    }
}
