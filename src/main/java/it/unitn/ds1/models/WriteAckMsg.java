package it.unitn.ds1.models;

import java.io.Serializable;

import it.unitn.ds1.utils.WriteId;

/**
 * Sent by the replicas, to acknowledge the write operation requested by the coordinator.
 */
public class WriteAckMsg implements Serializable {
    // ID of WriteMsg to ACK
    public final WriteId id;

    public WriteAckMsg(WriteId id) {
        this.id = id;
    }
}
