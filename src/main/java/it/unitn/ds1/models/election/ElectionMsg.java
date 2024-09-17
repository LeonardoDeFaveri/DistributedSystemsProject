package it.unitn.ds1.models.election;

import java.io.Serializable;
import java.util.*;

import it.unitn.ds1.utils.ElectionId;
import it.unitn.ds1.utils.WriteId;

/**
 * Represents an election message, sent from one replica to the next, collecting
 * replicas' IDs and their last update, to decide a new coordinator.
 */
public class ElectionMsg implements Serializable {
  public final ElectionId id;
  public final Map<Integer, WriteId> participants; // Contains pairs (ReplicaID, LastUpdate)

  public ElectionMsg(int epoch, int replicaID, WriteId lastUpdate, boolean isFirstRound) {
    this.id = new ElectionId(replicaID, epoch, isFirstRound);
    this.participants = new HashMap<>();
    this.participants.put(replicaID, lastUpdate);
  }

  /**
   * This constructor should only be used when an election messages passes
   * from the first to the second round. All the information of the original
   * messages are mainteined except for the round, which is set to false.
   * @param msg first round election message
   */
  public ElectionMsg(ElectionMsg msg) {
    this.id = new ElectionId(msg.id.initiator, msg.id.epoch, false);
    this.participants = msg.participants;
  }
}
