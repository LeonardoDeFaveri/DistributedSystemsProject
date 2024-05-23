package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent by the client to the replica, to update the value
 */
public class WriteMsg implements Serializable {
    public final int v;

    public WriteMsg(int v) {
        this.v = v;
    }
}