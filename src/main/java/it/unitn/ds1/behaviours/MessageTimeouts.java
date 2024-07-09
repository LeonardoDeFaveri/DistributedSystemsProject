package it.unitn.ds1.behaviours;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds1.Replica;
import it.unitn.ds1.models.crash_detection.HeartbeatMsg;
import it.unitn.ds1.models.crash_detection.HeartbeatReceivedMsg;
import it.unitn.ds1.models.crash_detection.WriteMsgReceivedMsg;
import it.unitn.ds1.models.crash_detection.WriteOkReceivedMsg;
import it.unitn.ds1.utils.Delays;
import it.unitn.ds1.utils.UpdateRequestId;
import scala.concurrent.duration.Duration;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MessageTimeouts {
    /**
     * For the coordinator: periodically sends heartbeat messages to
     * replicas.
     * For replicas: periodically sends heartbeat received messages
     * to themselves.
     */
    private Cancellable heartbeatTimer;
    /**
     * Timestamp of last time the replica received something from the coordinator.
     */
    private long lastContact;
    /**
     * For each UpdateRequestMsg received by a client, the ID is stored up
     * until the associated WriteMsg is received.
     */
    private final Set<UpdateRequestId> pendingUpdateRequests = new HashSet<>();
    private final Replica thisReplica;

    public MessageTimeouts(Replica thisReplica) {
        this.thisReplica = thisReplica;
    }

    public void startHeartbeatCoordinatorTimer() {
        // Begin sending heartbeat messages to replicas
        this.heartbeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(0, TimeUnit.SECONDS),
                Duration.create(Delays.SEND_HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new HeartbeatMsg(),
                getContext().system().dispatcher(),
                getSelf()
        );
        System.out.printf("[Co] Coordinator %s started\n", getSelf().path().name());
    }

    public void startHeartbeatReplicaTimer() {
        // Begins sending heartbeat to self
        this.heartbeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(Delays.RECEIVE_HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new HeartbeatReceivedMsg(),
                getContext().system().dispatcher(),
                getSelf()
        );
        this.resetLastContact();
        System.out.printf("[R] Replica %s started\n", getSelf().path().name());
    }

    /**
     * Stop the heartbeat timer
     */
    public void stopHeartbeatTimer() {
        if (this.heartbeatTimer != null) {
            this.heartbeatTimer.cancel();
        }
    }

    /**
     * Get the last contact we had with the coordinator
     */
    public long getLastContact() {
        return lastContact;
    }

    /**
     * Auxiliary method for resetting time of last contact with the coordinator.
     */
    public void resetLastContact() {
        this.lastContact = new Date().getTime();
    }

    /**
     * When one update is received, adds it to the set of pending updates, i.e.
     * the set of updates for which a WriteMsg has to be generated.
     */
    public void addPendingUpdate(UpdateRequestId updateRequestId) {
        this.pendingUpdateRequests.add(updateRequestId);
    }

    /**
     * When one update is received, remove from the pending updates
     */
    public void removePendingUpdate(UpdateRequestId updateRequestId) {
        this.pendingUpdateRequests.remove(updateRequestId);
    }

    //=== HANDLERS =============================================================
    /**
     * Timeout for the WriteMsg. If the pair is still in the map, the replica
     * has not responded in time.
     */
    public void onWriteMsgTimeoutReceivedMsg(WriteMsgReceivedMsg msg) {
        if (this.pendingUpdateRequests.contains(msg.updateRequestId)) {
            // No WriteMsg received, coordinator crashed
            thisReplica.recordCoordinatorCrash(
                    String.format("missed WriteMsg for write req %d from %s",
                            msg.updateRequestId.index,
                            msg.updateRequestId.client.path().name()
                    ));
        }
    }

    /**
     * When a HeartbeatMsg is received:
     * - Coordinator multicasts it to all replicas;
     * - Replicas reset they're time of last contact from the coordinator;
     */
    public void onHeartbeatMsg(HeartbeatMsg msg) {
        if (thisReplica.isCoordinator()) {
            // Sends an heartbeat to all replicas signaling that it's still
            // alive.
            // The coordinator should not send it to itself otherwise it would
            // keeps sending them infinitely.
            thisReplica.multicast(msg, true);
        } else {
            // Since a replica has received a heartbeat it knows the coordinator
            // is still alive
            this.resetLastContact();
        }
    }

    /**
     * Timeout for the WriteOk. If the pair is still in the map, the replica has
     * not responded in time.
     */
    public void onWriteOkTimeoutReceivedMsg(WriteOkReceivedMsg msg) {
        // Ignore WriteOks for messages sent is previous epochs as the crash of
        // that coordinator has already been detected and the nees to handle
        // again this message is known
        if (msg.writeMsgId.epoch < this.thisReplica.getEpoch()) {
            return;
        }

        if (!thisReplica.getWriteOks().remove(msg.writeMsgId)) {
            // No WriteOk received, coordinator crashed
            thisReplica.recordCoordinatorCrash(
                    String.format("missed WriteOk for epoch %d index %d",
                            msg.writeMsgId.epoch,
                            msg.writeMsgId.index
                    ));
        }
    }

    /**
     * Checks how much time has elapsed since the last message received
     * from the coordinator. If too much has elapsed, then a crash is detected.
     */
    public void onHeartbeatTimeoutReceivedMsg(HeartbeatReceivedMsg msg) {
        long now = new Date().getTime();
        long elapsed = now - this.getLastContact();
        if (elapsed > Delays.RECEIVE_HEARTBEAT_TIMEOUT) {
            // Too much time has passed since last hearing from the coordinator
            // The coordinator in crashed
            thisReplica.recordCoordinatorCrash(
                    String.format("missed HeartbeatMsg: %d elapsed",
                            elapsed
                    ));
        }
    }

    //=== AUXILIARIES ==========================================================
    private ActorContext getContext() {
        return thisReplica.getContext();
    }

    private ActorRef getSelf() {
        return thisReplica.getSelf();
    }
}
