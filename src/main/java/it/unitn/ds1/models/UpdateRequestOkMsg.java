package it.unitn.ds1.models;

import java.io.Serializable;

/**
 * This messages is sent by a replica back to client that sent it the
 * `UpdateRequestMsg` that initiated the update protocol. The message should
 * contain the index found inside that update request.
 */
public class UpdateRequestOkMsg implements Serializable {
    public final int id;

    public UpdateRequestOkMsg(int id) {
        this.id = id;
    }
}
