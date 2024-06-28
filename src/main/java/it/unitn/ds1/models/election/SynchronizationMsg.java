package it.unitn.ds1.models.election;

import java.io.Serializable;

public class SynchronizationMsg implements Serializable {
    public final int epoch;

    public SynchronizationMsg(int epoch) {
        this.epoch = epoch;
    }
}
