package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.behaviours.CoordinatorBehaviour;
import it.unitn.ds1.behaviours.MessageTimeouts;
import it.unitn.ds1.behaviours.ReplicaElectionBehaviour;
import it.unitn.ds1.models.ReadMsg;
import it.unitn.ds1.models.ReadOkMsg;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.UpdateRequestOkMsg;
import it.unitn.ds1.models.administratives.JoinGroupMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.crash_detection.*;
import it.unitn.ds1.models.election.*;
import it.unitn.ds1.models.update.WriteAckMsg;
import it.unitn.ds1.models.update.WriteMsg;
import it.unitn.ds1.models.update.WriteOkMsg;
import it.unitn.ds1.utils.Delays;
import it.unitn.ds1.utils.WriteId;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    static final int CRASH_CHANCES = 100;

    /**
     * All replicas in the system, whether they're active or not.
     */
    private final List<ActorRef> replicas = new ArrayList<>();
    /**
     * Set of replicas that have been detected as crashed.
     */
    private final Set<ActorRef> crashedReplicas = new HashSet<>();
    private final int replicaID;
    /**
     * Maps the ID of each WriteMsg received to the value that should be written.
     */
    private final Map<WriteId, Integer> writeRequests = new HashMap<>();
    /**
     * Keeps momentarily all WriteOks received. They will be later removed on
     * arrival on WriteOkReceivedMsgs.
     */
    private final Set<WriteId> writeOks = new HashSet<>();
    //=== CRASH DETECTION ======================================================
    private final MessageTimeouts timeoutsBehaviour = new MessageTimeouts(this);
    /**
     * Collects all the update requests received by clients so that they can
     * be later ACKed when the request has been successfully served.
     */
    private final Set<UpdateRequestMsg> updateRequests = new HashSet<>();

    //=== UPDATE PROTOCOL ======================================================
    /**
     * The behaviour of the replica during the election
     */
    private final ReplicaElectionBehaviour electionBehaviour = new ReplicaElectionBehaviour(this);
    /**
     * The behaviour of the replica acting as the coordinator
     */
    private final CoordinatorBehaviour coordinatorBehaviour = new CoordinatorBehaviour(this);
    //=== OTHERS ===============================================================
    private final Random numberGenerator = new Random(System.nanoTime());
    /**
     * Index of the coordinator replica inside replicas
     */
    private int coordinatorIndex;
    private boolean isCoordinator = false;
    /**
     * Current value of the replica.
     */
    private int value;
    /**
     * Current epoch.
     */
    private int epoch;
    /**
     * This is the ID of the last write that was applied. It's necessary to
     * prevent older writes to be applied after newer ones.
     */
    private WriteId lastWrite = new WriteId(-1, -1);


    public Replica(int replicaID, int value, int coordinatorIndex) {
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), value);
        this.coordinatorIndex = coordinatorIndex;
        this.value = value;
        this.replicaID = replicaID;

        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), value);
    }

    public static Props props(int replicaID, int v, int coordinatorIndex) {
        return Props.create(
                Replica.class,
                () -> new Replica(replicaID, v, coordinatorIndex)
        );
    }

    //=== Utility methods ======================================================

    /**
     * Sends msg to receiver with a random delay of [0, DELAY)ms.
     *
     * @param receiver receiver of the message
     * @param msg      message to send
     */
    public void tellWithDelay(ActorRef receiver, Serializable msg) {
        int delay = this.numberGenerator.nextInt(0, Delays.MAX_DELAY);
        getContext().system().scheduler().scheduleOnce(
                Duration.create(delay, TimeUnit.MILLISECONDS),
                receiver,
                msg,
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    /**
     * Multicasts a message to all replicas including itself.
     *
     * @param msg The message to send
     */
    public void multicast(Serializable msg) {
        multicast(msg, false);
    }

    /**
     * Multicasts a message to all replicas, possibly excluding itself.
     *
     * @param msg           The message to send
     * @param excludeItself Whether the replica should exclude itself from
     *                      the multicast or not
     */
    public void multicast(Serializable msg, boolean excludeItself) {
        var replicas = this.replicas.stream().filter(
                r -> !excludeItself || r != this.getSelf()
        ).toList();

        for (ActorRef replica : replicas) {
            this.tellWithDelay(replica, msg);
        }
    }

    //=== HANDLERS FOR INITIATION AND TERMINATION MESSAGES =====================
    private void onJoinGroupMsg(JoinGroupMsg msg) {
        this.replicas.addAll(msg.replicas);
        this.coordinatorBehaviour.setQuorum((this.replicas.size() / 2));

        this.isCoordinator = this.replicas.indexOf(this.getSelf()) == this.coordinatorIndex;
        if (this.isCoordinator) {
            getContext().become(createCoordinator());
        }
    }

    /**
     * When a StartMsg is received:
     * - Coordinator starts sending HeartbeatMsgs to replicas;
     * - Replicas starts checking for the liveness of the coordinator;
     */
    private void onStartMsg(StartMsg msg) {
        if (this.isCoordinator) {
            this.timeoutsBehaviour.startHeartbeatCoordinatorTimer();
        } else {
            this.timeoutsBehaviour.startHeartbeatReplicaTimer();
        }
    }

    //=== HANDLERS FOR UPDATE REQUESTS RELATED MESSAGES ========================

    /**
     * When a client sends a request to update the value of the replica
     */
    private void onUpdateRequest(UpdateRequestMsg msg) {
        // If the request comes from a client, register its arrival.
        // This replica will later have to send an ACK back to this client
        if (!this.replicas.contains(getSender())) {
            // Immediately inform the client of the receipt of the update request
            this.tellWithDelay(getSender(), new UpdateRequestOkMsg(msg.id.index));
            // Register the request, will be removed when the write is applied
            this.updateRequests.add(msg);
        }

        // Send the request to the coordinator
        var coordinator = this.replicas.get(this.coordinatorIndex);
        // Sends an ACK back to the client
        this.tellWithDelay(
                msg.id.client,
                new UpdateRequestOkMsg(msg.id.index)
        );

        this.tellWithDelay(coordinator, msg);

        // Registers this updateRequest and waits for the corresponding
        // WriteMsg from the coordinator
        this.timeoutsBehaviour.removePendingUpdate(msg.id);
        // Sets a timeout for the broadcast from the coordinator
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.WRITEMSG_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new WriteMsgReceivedMsg(msg.id),
                getContext().system().dispatcher(),
                getSelf()
        );

        System.out.printf(
                "[R] Replica %s forwarded write req to coordinator %s for %d in epoch %d\n",
                getSelf().path().name(),
                coordinator.path().name(),
                msg.value,
                this.epoch
        );
    }

    /**
     * The coordinator is requesting to write a new value to the replicas
     */
    private void onWriteMsg(WriteMsg msg) {
        // Removes this updateRequest from the set of pending ones
        this.timeoutsBehaviour.removePendingUpdate(msg.updateRequestId);
        // Add the request to the list, so that it is ready if the coordinator
        // requests the update
        this.writeRequests.put(msg.id, msg.value);
        // Send the acknowledgement to the coordinator
        this.tellWithDelay(
                getSender(),
                new WriteAckMsg(msg.id)
        );

        System.out.printf(
                "[R] Write requested by the coordinator %s to %s for %d in epoch %d and index %d\n",
                getSender().path().name(),
                this.getSelf().path().name(),
                msg.value,
                msg.id.epoch,
                msg.id.index
        );

        // The replicas sets a timeout for the expected WriteOk message from
        // the coordinator
        ActorRef client = null;
        if (msg.updateRequestId != null) {
            client = msg.updateRequestId.client;
        }
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.WRITEOK_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new WriteOkReceivedMsg(client, msg.id),
                getContext().system().dispatcher(),
                getSelf()
        );

        // A replica resets its last contact with the coordinator on every message
        this.timeoutsBehaviour.resetLastContact();
    }

    /**
     * The coordinator sent the write ok message, so the replicas can apply the
     * write
     */
    private void onWriteOkMsg(WriteOkMsg msg) {
        // If the epoch of the write is not the current epoch, ignore the message
        if (msg.id.epoch != this.epoch)
            // Resetting last contact here would be wrong since the received
            // message comes from an already crashed coordinator
            return;

        // If the pair epoch-index is not in the map, ignore the message
        if (!this.writeRequests.containsKey(msg.id))
            return;

        // Registers the arrival of the ok for a later check
        this.writeOks.add(msg.id);

        /*
          The OK has been received, so the request is removed.
          If a request has not received the Ok, it means that the coordinator crashed, so the update will be sent
          to the new coordinator.
         */
        var updateMsg = this.updateRequests.stream()
                .filter(u -> u.id.equals(msg.updateRequestId))
                .findFirst();
        updateMsg.ifPresent(this.updateRequests::remove);

        // If received message is for a write request older than the last served,
        // ignore it
        if (msg.id.isPriorOrEqualTo(this.lastWrite)) {
            return;
        }

        // Apply the write
        this.value = this.writeRequests.get(msg.id);
        // Update the last write
        this.lastWrite = msg.id;

        System.out.printf(
                "[R] [%s] Applied the write %d in epoch %d with value %d\n",
                this.self().path().name(),
                msg.id.index,
                msg.id.epoch,
                this.value
        );

        this.timeoutsBehaviour.resetLastContact();
    }

    //=== HANDLERS FOR READ REQUEST RELATED MESSAGES ===========================

    /**
     * The client is requesting to read the value of the replica
     */
    private void onReadMsg(ReadMsg msg) {
        ActorRef sender = getSender();
        this.tellWithDelay(sender, new ReadOkMsg(this.value, msg.id));

        System.out.printf(
                "[C] Client %s read req to %s\n",
                getSender().path().name(),
                this.getSelf().path().name()
        );
    }

    //=== METHODS AND HANDLERS FOR THE ELECTION PROTOCOL =======================

    /**
     * Prepares this replica for carrying out the election.
     */
    public void beginElection() {
        getContext().become(createElection());
        this.electionBehaviour.setElectionUnderway(true);
    }

    /**
     * When the coordinator changes, the epoch is increased and writes starts
     * again from 0.
     */
    public void onCoordinatorChange(int epoch) {
        this.epoch = epoch;
        this.coordinatorBehaviour.onCoordinatorChange();
        this.electionBehaviour.setElectionUnderway(false);
        this.updateRequests.forEach(this::onUpdateRequest); // Send all the update requests for which a WriteOk was not received to the new coordinator
        this.electionBehaviour.getQueuedUpdates().forEach(this::onUpdateRequest); // Send all the queued updates to the new coordinator
        this.electionBehaviour.getQueuedUpdates().clear();
    }

    /**
     * Returns the next node on the ring.
     *
     * @return The next node on the ring
     */
    public ActorRef getNextNode() {
        int currentIndex = this.replicas.indexOf(getSelf());
        int nextIndex = (currentIndex + 1) % this.replicas.size();
        ActorRef replica = this.replicas.get(nextIndex);
        // Go to the next replica up until it is not crashed, or if it has the same index as the coordinator (which has crashed at this point)
        while (this.crashedReplicas.contains(replica) || nextIndex == coordinatorIndex) {
            nextIndex = (nextIndex + 1) % this.replicas.size();
            replica = this.replicas.get(nextIndex);
        }
        return replica;
    }

    //=== HANDLERS FOR CRASH DETECTION MESSAGES ================================

    /**
     * When a CrashMsg is received there's certain chance of actually crashing.
     */
    private void onCrashMsg(CrashMsg msg) {
        int chance = this.numberGenerator.nextInt(CRASH_CHANCES);

        // Each replica has a 10% chance of crashing
        if (chance >= 10) {
            getSender().tell(new CrashResponseMsg(false), getSelf());

            System.out.printf(
                    "[R] Replica %s received crash message and DIDN'T CRASH\n",
                    getSelf().path().name()
            );
        } else {
            getSender().tell(new CrashResponseMsg(true), getSelf());
            // The replica has crashed and will not respond to messages anymore
            getContext().become(createCrashed());
            // Stop sending heartbeat messages
            this.timeoutsBehaviour.stopHeartbeatTimer();

            System.out.printf(
                    "[R] Replica %s received crash message and CRASHED\n",
                    getSelf().path().name()
            );
        }
    }

    /**
     * Auxiliary method for recording the crash of the coordinator.
     * It stops the timer for heartbeats and initiates the election protocol.
     */
    public void recordCoordinatorCrash(String cause) {
        this.crashedReplicas.add(
                this.replicas.get(this.coordinatorIndex)
        );

        // Stop sending heartbeat received messages
        this.timeoutsBehaviour.stopHeartbeatTimer();

        System.out.printf(
                "[R] Coordinator crash detected by replica %s on %s\n",
                getSelf().path().name(),
                cause
        );

        // Initiate election protocol
        this.beginElection();
        this.electionBehaviour.sendElectionMessage();
    }

    //=== SETUP OF MESSAGES HANDLERS ===========================================

    /**
     * Message listeners for an active replica which is not the coordinator.
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(StartMsg.class, this::onStartMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .match(ElectionMsg.class, this.electionBehaviour::onElectionMsg)
                .match(HeartbeatMsg.class, this.timeoutsBehaviour::onHeartbeatMsg)
                .match(HeartbeatReceivedMsg.class, this.timeoutsBehaviour::onHeartbeatTimeoutReceivedMsg)
                .match(WriteMsgReceivedMsg.class, this.timeoutsBehaviour::onWriteMsgTimeoutReceivedMsg)
                .match(WriteOkReceivedMsg.class, this.timeoutsBehaviour::onWriteOkTimeoutReceivedMsg)
                .match(CrashMsg.class, this::onCrashMsg)
                .build();
    }

    /**
     * Message listeners for the coordinator.
     */
    public AbstractActor.Receive createCoordinator() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(StartMsg.class, this::onStartMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this.coordinatorBehaviour::onUpdateRequest)
                .match(HeartbeatMsg.class, this.timeoutsBehaviour::onHeartbeatMsg)
                .match(WriteAckMsg.class, this.coordinatorBehaviour::onWriteAckMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .match(CrashMsg.class, this::onCrashMsg)
                .build();
    }

    /**
     * Message listeners for a replica busy in an election
     */
    public AbstractActor.Receive createElection() {
        return receiveBuilder()
                .match(ReadMsg.class, this::onReadMsg) // The read is served by the replica, so it's the same
                .match(CrashMsg.class, this::onCrashMsg)
                .match(LostUpdatesMsg.class, this.electionBehaviour::onLostUpdatesMsg)
                .match(UpdateRequestMsg.class, this.electionBehaviour::onUpdateRequestMsg)
                .match(ElectionMsg.class, this.electionBehaviour::onElectionMsg)
                .match(CoordinatorMsg.class, this.electionBehaviour::onCoordinatorMsg)
                .match(SynchronizationMsg.class, this.electionBehaviour::onSynchronizationMsg)
                .match(ElectionAckMsg.class, this.electionBehaviour::onElectionAckMsg)
                .match(CoordinatorAckMsg.class, this.electionBehaviour::onCoordinatorAckMsg)
                .match(ElectionAckReceivedMsg.class, this.electionBehaviour::onElectionAckTimeoutReceivedMsg)
                .match(CoordinatorAckReceivedMsg.class, this.electionBehaviour::onCoordinatorAckTimeoutReceivedMsg)
                .build();
    }

    /**
     * Listeners for a crashed replica. A crashed replicas doesn't handle any
     * message.
     */
    final AbstractActor.Receive createCrashed() {
        return receiveBuilder()
                .build();
    }

    /**
     * Getter and setters
     */

    public int getReplicaID() {
        return this.replicaID;
    }

    public int getCoordinatorIndex() {
        return coordinatorIndex;
    }

    public void setCoordinatorIndex(int coordinatorIndex) {
        this.coordinatorIndex = coordinatorIndex;
    }

    public WriteId getLastWrite() {
        return lastWrite;
    }

    public List<ActorRef> getReplicas() {
        return replicas;
    }

    public void setIsCoordinator(boolean coordinator) {
        isCoordinator = coordinator;
    }

    public Set<ActorRef> getCrashedReplicas() {
        return crashedReplicas;
    }

    public Map<WriteId, Integer> getWriteRequests() {
        return writeRequests;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public boolean isCoordinator() {
        return isCoordinator;
    }

    public Set<WriteId> getWriteOks() {
        return writeOks;
    }

    public void resetLastContact() {
        this.timeoutsBehaviour.resetLastContact();
    }

    public int getEpoch() {
        return this.epoch;
    }
}