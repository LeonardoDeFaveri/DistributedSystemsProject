package it.unitn.ds1.models.election;

import java.io.Serializable;
import java.util.*;

import it.unitn.ds1.utils.WriteId;

/**
 * Represents an election message, sent from one replica to the next, collecting
 * replicas' IDs and their last update, to decide a new coordinator.
 */
public class ElectionMsg implements Serializable {
  public final int index;
  public final Map<Integer, WriteId> participants; // Contains pairs (ReplicaID, LastUpdate)

  public ElectionMsg(int index, int replicaID, WriteId lastUpdate) {
    this.index = index;
    this.participants = new HashMap<>();
    this.participants.put(replicaID, lastUpdate);
  }
}
