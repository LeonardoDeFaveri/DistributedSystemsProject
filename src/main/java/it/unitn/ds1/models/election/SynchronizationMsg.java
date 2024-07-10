package it.unitn.ds1.models.election;

import java.io.Serializable;
import java.util.List;

import it.unitn.ds1.models.update.WriteMsg;

public class SynchronizationMsg implements Serializable {
    public final int epoch;
    public final List<WriteMsg> missedUpdates;

    public SynchronizationMsg(int epoch, List<WriteMsg> missedUpdates) {
        this.epoch = epoch;
        this.missedUpdates = missedUpdates;
    }
}
