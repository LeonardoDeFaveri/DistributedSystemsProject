package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent by the coordinator, to apply the write operation.
 */
public class WriteOkMsg implements Serializable {
    public final int epoch; // The epoch of the coordinator
    public final int writeIndex; // The index of the write operation

    public WriteOkMsg(int epoch, int writeIndex) {
        this.epoch = epoch;
        this.writeIndex = writeIndex;
    }
}
