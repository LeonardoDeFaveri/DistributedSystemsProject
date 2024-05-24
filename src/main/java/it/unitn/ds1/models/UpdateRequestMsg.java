package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent by the client to the replica, to update the value.
 */
public class UpdateRequestMsg implements Serializable {
    public final int v;

    public UpdateRequestMsg(int v) {
        this.v = v;
    }
}