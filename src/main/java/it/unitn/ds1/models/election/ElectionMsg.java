package it.unitn.ds1.models.election;

import java.io.Serializable;
import java.util.*;

/**
 * Represents an election message, sent from one replica to the next, collecting
 * replicas' IDs and their last update, to decide a new coordinator.
 */
public class ElectionMsg implements Serializable {
  public Map<Integer, LastUpdate> participants; // Contains pairs (ReplicaID, LastUpdate)

  public ElectionMsg(int replicaID, LastUpdate lastUpdate) {
    this.participants = new HashMap<>();
    this.participants.put(replicaID, lastUpdate);
  }

  /**
   * Epoch and writeIndex of the most recent update for a replica
   */
  public static class LastUpdate implements Serializable {
    public int epoch;
    public int writeIndex;

    public LastUpdate(int epoch, int writeIndex) {
      this.epoch = epoch;
      this.writeIndex = writeIndex;
    }
  }
}
