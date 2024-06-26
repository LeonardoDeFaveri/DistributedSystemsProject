package it.unitn.ds1.models.election;

import java.io.Serializable;
import java.util.Map;

import it.unitn.ds1.utils.WriteId;

/**
 * Sent when a new coordinator has been chosen after the election algorithm.
 */
public class CoordinatorMsg implements Serializable {
    public final int index;
    public final int coordinatorID;
    public final int senderID;
    public Map<Integer, WriteId> participants; // Contains pairs (ReplicaID, WriteId)

    public CoordinatorMsg(int index, int coordinatorID, int senderID, Map<Integer, WriteId> participants) {
        this.index = index;
        this.coordinatorID = coordinatorID;
        this.senderID = senderID;
        this.participants = participants;
    }
}
