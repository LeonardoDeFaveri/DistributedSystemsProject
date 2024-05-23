package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent by the coordinator, to write a new value to the replicas.
 */
public class WriteMsg implements Serializable {
    public final int v; // The new value to write
    public final int epoch; // The epoch (current coordinator)
    public final int writeIndex; // The index of the write operation

    public WriteMsg(int v, int e, int i) {
        this.v = v;
        this.epoch = e;
        this.writeIndex = i;
    }
}
