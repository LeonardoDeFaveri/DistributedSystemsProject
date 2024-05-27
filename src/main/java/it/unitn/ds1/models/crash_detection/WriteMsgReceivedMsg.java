package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;
import java.util.AbstractMap;

public class WriteMsgReceivedMsg implements Serializable {
    public final AbstractMap.SimpleEntry<Integer, Integer> writeMsg;
    
    public WriteMsgReceivedMsg(AbstractMap.SimpleEntry<Integer, Integer> writeMsg) {
        this.writeMsg = writeMsg;
    }
}
