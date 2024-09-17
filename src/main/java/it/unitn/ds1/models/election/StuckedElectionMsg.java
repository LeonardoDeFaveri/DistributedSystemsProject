package it.unitn.ds1.models.election;

import java.io.Serializable;

public class StuckedElectionMsg implements Serializable {
    /**
     * Epoch that should be closed by the election associated to this
     * message.
     */
    public final int epoch;

    public StuckedElectionMsg(int epoch) {
        this.epoch = epoch;
    }
}
