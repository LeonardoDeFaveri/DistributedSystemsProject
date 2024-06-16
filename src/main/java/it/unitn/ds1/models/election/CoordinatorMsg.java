package it.unitn.ds1.models.election;

import java.io.Serializable;
import java.util.Map;

/**
 * Sent when a new coordinator has been chosen after the election algorithm.
 */
public class CoordinatorMsg implements Serializable {
    public final int coordinatorID;
    public final int senderID;
    public Map<Integer, ElectionMsg.LastUpdate> participants; // Contains pairs (ReplicaID, LastUpdate)

    public CoordinatorMsg(int coordinatorID, int senderID, Map<Integer, ElectionMsg.LastUpdate> participants) {
        this.coordinatorID = coordinatorID;
        this.senderID = senderID;
        this.participants = participants;
    }
}
