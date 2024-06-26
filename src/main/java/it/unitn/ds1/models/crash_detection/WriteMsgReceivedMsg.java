package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

import it.unitn.ds1.utils.UpdateRequestId;

public class WriteMsgReceivedMsg implements Serializable {
    // UpdateRequest that caused the associated WriteMsg to be generated
    public final UpdateRequestId updateRequestId;
    
    public WriteMsgReceivedMsg(UpdateRequestId updateRequestId) {
        this.updateRequestId = updateRequestId;
    }
}
