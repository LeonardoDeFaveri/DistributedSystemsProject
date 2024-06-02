package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

public class UpdateRequestOkReceivedMsg implements Serializable {
    public final int index;

    public UpdateRequestOkReceivedMsg(int index) {
        this.index = index;
    }
}
