package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;
import java.util.AbstractMap;

/**
 * This message is sent by a replica to itself when a timeout expires and if
 * the requested writeOk message has not been received before this message, then
 * the coordinator is marked as crashed.
 */
public class WriteOkReceivedMsg implements Serializable {
    public final AbstractMap.SimpleEntry<Integer, Integer> writeMsg;

    public WriteOkReceivedMsg(AbstractMap.SimpleEntry<Integer, Integer> writeMsg) {
        this.writeMsg = writeMsg;
    }
}
