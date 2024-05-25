package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent when a new coordinator has been chosen after the election algorithm.
 */
public class CoordinatorMsg implements Serializable {
    public final int coordinatorID;
    public final int senderID;

    public CoordinatorMsg(int coordinatorID, int senderID) {
        this.coordinatorID = coordinatorID;
        this.senderID = senderID;
    }
}
