package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * Sent by the replica to the client, in response to a READ request.
 */
public class ReadOkMsg implements Serializable {
    public final int value;
    public final int id;

    public ReadOkMsg(int value, int id) {
        this.value = value;
        this.id = id;
    }
}
