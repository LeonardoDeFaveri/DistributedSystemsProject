package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent by the
 */
public class WriteMsg implements Serializable {
    public final int v;

    public WriteMsg(int v) {
        this.v = v;
    }
}
