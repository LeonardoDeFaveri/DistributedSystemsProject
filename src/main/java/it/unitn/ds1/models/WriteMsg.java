package it.unitn.ds1.models;

import java.io.Serializable;

import it.unitn.ds1.utils.UpdateRequestId;
import it.unitn.ds1.utils.WriteId;

/**
 * Sent by the coordinator, to write a new value to the replicas.
 */
public class WriteMsg implements Serializable {
    public final UpdateRequestId updateRequestId;
    public final WriteId id; // Identifier <epoch, index>
    public final int value; // The new value to write

    public WriteMsg(UpdateRequestId updateRequestId, WriteId id, int value) {
        this.updateRequestId = updateRequestId;
        this.id = id;
        this.value = value;
    }
}
