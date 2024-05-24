package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent by the replicas, to acknowledge the write operation requested by the coordinator.
 */
public class WriteAckMsg implements Serializable {
    public final int epoch; // Epoch of the coordinator
    public final int writeIndex; // Index of the write operation

    public WriteAckMsg(int epoch, int writeIndex) {
        this.epoch = epoch;
        this.writeIndex = writeIndex;
    }
}
