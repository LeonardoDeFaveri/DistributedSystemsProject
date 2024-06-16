package it.unitn.ds1.models.update;

import java.io.Serializable;

import it.unitn.ds1.utils.UpdateRequestId;
import it.unitn.ds1.utils.WriteId;

/**
 * Sent by the coordinator, to apply the write operation.
 */
public class WriteOkMsg implements Serializable {
    public final WriteId id;
    public final UpdateRequestId updateRequestId;

    public WriteOkMsg(WriteId id, UpdateRequestId updateRequestId) {
        this.id = id;
        this.updateRequestId = updateRequestId;
    }
}
