package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

import it.unitn.ds1.utils.UpdateRequestId;

public class WriteMsgReceivedMsg implements Serializable {
    // UpdateRequest that caused the associated WriteMsg to be generated
    public final UpdateRequestId updateRequestId;
    // In what epoch was this WriteMsg expected? Maybe the coordinator is crashed
    // and there's a new one handling the request
    public final int epoch;
    
    public WriteMsgReceivedMsg(UpdateRequestId updateRequestId, int epoch) {
        this.updateRequestId = updateRequestId;
        this.epoch = epoch;
    }
}
