package it.unitn.ds1.models.crash_detection;

import java.io.Serializable;

import akka.actor.ActorRef;
import it.unitn.ds1.utils.WriteId;

/**
 * This message is sent by a replica to itself when a timeout expires and if
 * the requested writeOk message has not been received before this message, then
 * the coordinator is marked as crashed.
 */
public class WriteOkReceivedMsg implements Serializable {
    public final ActorRef client;
    public final WriteId writeMsgId;

    public WriteOkReceivedMsg(ActorRef client, WriteId writeMsgId) {
        this.client = client;
        this.writeMsgId = writeMsgId;
    }
}
