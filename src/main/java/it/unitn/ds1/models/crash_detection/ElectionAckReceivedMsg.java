package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

import it.unitn.ds1.models.election.ElectionMsg;

public class ElectionAckReceivedMsg implements Serializable {
    public final ElectionMsg msg;

    public ElectionAckReceivedMsg(ElectionMsg msg) {
        this.msg = msg;
    }
}
