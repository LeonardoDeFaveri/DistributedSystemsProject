package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

import it.unitn.ds1.models.election.CoordinatorMsg;

public class CoordinatorAckReceivedMsg implements Serializable {
    public final CoordinatorMsg msg;

    public CoordinatorAckReceivedMsg(CoordinatorMsg msg) {
        this.msg = msg;
    }
}
