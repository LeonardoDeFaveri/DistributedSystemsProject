package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

/**
 * This message is sent by crashed replicas to signal they're actually crashed
 * to the crash manager.
 */
public class CrashResponseMsg implements Serializable{
    public final boolean isCrashed;

    public CrashResponseMsg(boolean isCrashed) {
        this.isCrashed = isCrashed;
    }
}
