package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

public class ReadOkReceivedMsg implements Serializable {
    /**
     * Id of the ReadMsg associated to the ReadMsg to check for confirmation.
     */
    public final int id;

    public ReadOkReceivedMsg(int id) {
        this.id = id;
    }
}
