package it.unitn.ds1;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import it.unitn.ds1.models.*;
import it.unitn.ds1.models.administratives.JoinGroupMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.crash_detection.*;
import it.unitn.ds1.models.election.*;
import it.unitn.ds1.models.update.*;
import it.unitn.ds1.utils.*;
import scala.concurrent.duration.Duration;

import java.util.stream.Collectors;

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
     * Index of the coordinator replica inside replicas
     */
    private int coordinatorIndex;
    private boolean isCoordinator = false;
  
    /**
     * Minimum number of nodes that must agree on a Write.
     */
    private int quorum = 0;
    /**
     * Current value of the replica.
     */
    private int value;
    /**
     * Current epoch.
     */
    private int epoch;
    /**
     * Index to be used for next WriteMsg created.
     */
    private int writeIndex = 0;

    //=== UPDATE PROTOCOL ======================================================
    /**
     * This is the ID of the last write that was applied. It's necessary to
     * prevent older writes to be applied after newer ones.
     */
    private WriteId lastWrite = new WriteId(-1, -1);
    /**
     * For each Write collects ACKs from replicas.
     */
    private final Map<WriteId, Set<ActorRef>> writeAcksMap = new HashMap<>();
    /**
     * Maps the ID of each WriteMsg received to the value that should be written.
     */
    private final Map<WriteId, Integer> writeRequests = new HashMap<>();
    /**
     * Keeps momentarily all WriteOks received. They will be later removed on
     * arrival on WriteOkReceivedMsgs.
     */
    private final Set<WriteId> writeOks = new HashSet<>();
    /**
     * Index of thhe write we are currently collecting ACKs for.
     */
    private int currentWriteToAck = 0;

    //=== ELECTION PROTOCOL ====================================================
    /**
     * Each election is identified by an index. This is necessary for ACKs.
     */
    private int electionIndex = 0;
    /**
     * True if a new coordinator is being chosen.
     */
    private boolean isElectionUnderway = false;
    /**
     * For each replica keeps track of its last applied write. ReplicaIDs are
     * used as keys and values WriteIds.
     */
    private Map<Integer, WriteId> lastWriteForReplica = new HashMap<>();
    /**
     * If an election is underway, incoming requests can't be served until the
     * new coordinator is chosen, so each request is deferred and served after
     * the election.
     */
    private Set<UpdateRequestMsg> deferredUpdateRequests = new HashSet<>();

    //=== CRASH DETECTION ======================================================
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
    /**
     * Maps the ID of each WriteMsg to the ID of the UpdateRequestMsg that
     * caused it.
     */
    private final Map<WriteId, UpdateRequestId> writesToUpdates = new HashMap<>();
    /**
     * Collects all the update requests received by clients so that they can
     * be later ACKed when the request has been succesfully served.
     */
    private final Set<UpdateRequestId> updateRequests = new HashSet<>();
    /**
     * Every ElectionMsg sent must be ACKed. Pairs (sender, index) of the ACK
     * are stored and later checked.
     */    
    private final Set<Map.Entry<ActorRef, Integer>> pendingElectionAcks = new HashSet<>();
    /**
     * Every CoordinatorMsg sent must be ACKed. Pairs (sender, index) of the ACK
     * are stored and later checked.
     */    
    private final Set<Map.Entry<ActorRef, Integer>> pendingCoordinatorAcks = new HashSet<>();
    /**
     * The behaviour of the replica during the election
     */
    private final ReplicaElectionBehaviour electionBehaviour = new ReplicaElectionBehaviour(this);
    
    //=== OTHERS ===============================================================
    private final Random numberGenerator = new Random(System.nanoTime());

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
     * @param receiver receiver of the message
     * @param msg message to send
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

    private void tellWithDelay(ActorRef receiver, Serializable msg, ActorRef sender) {
        int delay = this.numberGenerator.nextInt(0, Delays.MAX_DELAY);
        getContext().system().scheduler().scheduleOnce(
            Duration.create(delay, TimeUnit.MILLISECONDS),
            receiver,
            msg,
            getContext().system().dispatcher(),
            sender
        );
    }

    /**
     * Multicasts a message to all replicas including itself.
     * @param msg The message to send
     */
    private void multicast(Serializable msg) {
        multicast(msg, false);
    }

    /**
     * Multicasts a message to all replicas, possibly excluding itself.
     * @param msg The message to send
     * @param excludeItself Whether the replica should exclude itself from
     * the multicast or not
     */
    private void multicast(Serializable msg, boolean excludeItself) {
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
        this.quorum = (this.replicas.size() / 2); // ! No + 1, because one is itself

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
        } else {
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
    }

    //=== HANDLERS FOR UPDATE REQUESTS RELATED MESSAGES ========================
    /**
     * When a client sends a request to update the value of the replica
     */
    private void onUpdateRequest(UpdateRequestMsg msg) {
        // If the request comes from a client, register its arrival.
        // This replica will later have to send an ACK back to this client
        if (!this.replicas.contains(getSender())) {
            this.updateRequests.add(msg.id);
            System.out.printf(
                "[R] Replica %s registered write request %d for %d from client %s\n",
                getSelf().path().name(),
                msg.id.index,
                msg.value,
                msg.id.client.path().name()
            );
        }

        // This replica is busy electing a new coordinator, so defer the serving
        // of this request
        if (this.isElectionUnderway) {
            this.deferredUpdateRequests.add(msg);
            System.out.printf(
                "Replica %s deferred request %d from %s\n",
                this.getSelf().path().name(),
                msg.id.index,
                msg.id.client.path().name()
            );
            return;
        }

        // If the replica is not the coordinator
        if (!this.isCoordinator) {
            // Send the request to the coordinator
            var coordinator = this.replicas.get(this.coordinatorIndex);
            this.tellWithDelay(coordinator, msg);

            // Registers this updateRequest and waits for the corresponding
            // WriteMsg from the coordinator
            this.pendingUpdateRequests.add(msg.id);
            // Sets a timeout for the broadcast from the coordinator
            getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.WRITEMSG_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new WriteMsgReceivedMsg(msg.id),
                getContext().system().dispatcher(),
                getSelf()
            );

            System.out.printf(
                "[R] Replica %s forwared write req to coordinator %s for %d in epoch %d with index %d\n",
                getSelf().path().name(),
                coordinator.path().name(),
                msg.value,
                this.epoch,
                this.writeIndex
            );

            this.writeIndex++;
            return;
        }

        // The pair associated to the new writeMsg for this update request
        var writeId = new WriteId(this.epoch, this.writeIndex);
        // Implement the quorum protocol. The coordinator asks all the replicas
        // to update
        multicast(new WriteMsg(msg.id, writeId, msg.value));

        // Associated this write to the update request that caused it
        this.writesToUpdates.putIfAbsent(writeId, msg.id);

        // Add the new write request to the map, so that the acks can be received
        this.writeAcksMap.putIfAbsent(writeId, new HashSet<>());
        this.writeIndex++;
    }

    /**
     * When the coordinator receives an ack from a replica
     */
    private void onWriteAckMsg(WriteAckMsg msg) {
        // If the epoch of the write is not the current epoch, ignore the message
        if (msg.id.epoch != this.epoch)
            return;

        // The OK has already been sent, as the quorum was reached.
        // ACKs from other replicas for the same write should be ignored.
        if (!this.writeAcksMap.containsKey(msg.id)) {
            return;
        }

        // Add the sender to the list
        this.writeAcksMap.get(msg.id).add(getSender());

        // Send all the messages that have been acked in FIFO order!
        sendAllAckedMessages();

        System.out.printf(
            "[Co] Received ack from %s for %d in epoch %d\n",
            getSender().path().name(),
            msg.id.index,
            msg.id.epoch
        );
    }

    /**
     * The first message to be served is the currentWriteToAck index.
     * When the message is sent to the replicas, serve all the successive messages
     */
    private void sendAllAckedMessages() {
        // Starting from the first message to send, if the quorum has been reached,
        // send the message. Then, go to the next message (continue until the
        // last write has been reached).
        // If any of the writes didn't reach the quorum, stop!
        while (this.currentWriteToAck < this.writeIndex) {
            var writeId = new WriteId(this.epoch, this.currentWriteToAck);
            var updateRequestId = this.writesToUpdates.get(writeId);
            if (this.writeAcksMap.containsKey(writeId) && this.writeAcksMap.get(writeId).size() >= this.quorum) {
                multicast(new WriteOkMsg(writeId, updateRequestId));
                this.writeAcksMap.remove(writeId);
                this.currentWriteToAck++;
            } else {
                break;
            }
        }
    }

    /**
     * The coordinator is requesting to write a new value to the replicas
     */
    private void onWriteMsg(WriteMsg msg) {
        // Removes this updateRequest from the set of pending ones
        this.pendingUpdateRequests.remove(msg.updateRequestId);
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
        this.resetLastContact();
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

        // Checks if this replicas has to inform the original client of the
        // completed update [Must be done regardless of the subsequent check on
        // request age to avoid wrong crash detection from the client]
        if (this.updateRequests.contains(msg.updateRequestId)) {
            this.updateRequests.remove(msg.updateRequestId);
            // Sends an ACK back to the client
            this.tellWithDelay(
                msg.updateRequestId.client,
                new UpdateRequestOkMsg(msg.updateRequestId.index)
            );
        }

        // If received message is for a write request older then the last served,
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

        this.resetLastContact();
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
        this.deferredUpdateRequests = new HashSet<>();
        this.isElectionUnderway = true;
    }

    /**
     * When the coordinator changes, the epoch is increased and writes starts
     * again from 0.
     */
    public void onCoordinatorChange() {
        this.epoch++;
        this.writeIndex = 0;
        this.isElectionUnderway = false;
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
        // Go to the next replica up until it is not crashed
        while (this.crashedReplicas.contains(replica)) {
            nextIndex = (nextIndex + 1) % this.replicas.size();
            replica = this.replicas.get(nextIndex);
        }
        return replica;
    }

    /**
     * Sends an ElectionMsg to the next node.
     */
    public void sendElectionMessage() {
        var nextNode = this.getNextNode();
        var msg = new ElectionMsg(this.electionIndex, this.replicaID, this.lastWrite);
        this.tellWithDelay(nextNode, msg);
        this.electionIndex++;

        // For each election message the sender expects an ACK back
        getContext().system().scheduler().scheduleOnce(
            Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
            getSelf(),
            new ElectionAckReceivedMsg(msg),
            getContext().system().dispatcher(),
            nextNode
        );
    }

    /**
     * When an ElectionMsg is received:
     * - If the message already contains this replicaID, then change the type to
     * Coordinator, and set the coordinatorID to the node which is the most
     * updated in the list (highest epoch and writeIndex), and take the node with
     * the highest ID in case of a tie;
     * - Otherwise, add the replicaID of this node + the last update to the list,
     * then propagate to the next node;
     */
    private void onElectionMsg(ElectionMsg msg) {
        if (!this.isElectionUnderway) {
            this.beginElection();
            if (msg.index > this.electionIndex) {
                this.electionIndex = msg.index;
            }
        }

        // Sends back the ACK
        this.tellWithDelay(getSender(), new ElectionAckMsg(msg.index));

        System.out.printf(
            "[R: %d] election message received from replica %s with content: %s\n",
            this.replicaID,
            getSender().path().name(),
            msg.participants.entrySet().stream().map(
                (content) ->
                    String.format(
                        "{ replicaID: %d, lastUpdate: (%d, %d) }",
                        content.getKey(),
                        content.getValue().epoch,
                        content.getValue().index
                    )
                ).collect(Collectors.joining(", ")
            )
        );

        ActorRef nextNode = getNextNode();
        // When a node receives the election message, and the message already
        // contains the node's ID, then change the message type to COORDINATOR.
        // The new leader is the node with the latest update
        // highest (epoch, writeIndex), and highest replicaID.
        if (msg.participants.containsKey(this.replicaID)) {
            var mostUpdated = msg.participants.entrySet().stream().reduce(Utils::getNewCoordinatorIndex);
            this.coordinatorIndex = mostUpdated.get().getKey();

            System.out.printf(
                "[R: %d] New coordinator found: %d\n",
                this.replicaID,
                this.coordinatorIndex
            );
            if (this.coordinatorIndex == this.replicaID) {
                this.lastWriteForReplica = msg.participants;
                sendSynchronizationMessage();
                sendLostUpdates();
                this.onCoordinatorChange();
                return;
            }

            CoordinatorMsg coordinatorMsg = new CoordinatorMsg(
                msg.index,
                this.coordinatorIndex,
                this.replicaID,
                msg.participants
            );
            this.tellWithDelay(nextNode, coordinatorMsg);
            getContext().system().scheduler().scheduleOnce(
                Duration.create(Delays.COORDINATOR_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorAckReceivedMsg(coordinatorMsg),
                getContext().system().dispatcher(),
                nextNode
            );
            return;
        }
        // If it's an election message, and my ID is not in the list, add it and
        // propagate to the next node.
        msg.participants.put(this.replicaID, this.lastWrite);
        this.tellWithDelay(nextNode, msg);
        getContext().system().scheduler().scheduleOnce(
            Duration.create(Delays.ELECTION_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
            getSelf(),
            new ElectionAckReceivedMsg(msg),
            getContext().system().dispatcher(),
            nextNode
        );
    }

    /**
     * The Election message has been received by all the nodes, and the
     * coordinator has been elected. The coordinator sends a message to all the
     * nodes to synchronize the epoch and the writeIndex.
     */
    private void onCoordinatorMsg(CoordinatorMsg msg) {
        this.tellWithDelay(getSender(), new CoordinatorAckMsg(msg.index));

        // The replica is the sender of the message, so it already has the
        // coordinator index
        if (msg.senderID == this.replicaID)
            return;
        // This replica is the new coordinator
        if (msg.coordinatorID == this.replicaID) {
            this.lastWriteForReplica = msg.participants;
            sendSynchronizationMessage();
            sendLostUpdates();
            return;
        }
        System.out.printf(
            "[R%d] received new coordinator %d from %d%n",
            this.replicaID, msg.coordinatorID, msg.senderID
        );
        this.coordinatorIndex = msg.coordinatorID; // Set the new coordinator

        ActorRef nextNode = getNextNode();
        // Forward the message to the next node
        this.tellWithDelay(nextNode, msg);
        
        getContext().system().scheduler().scheduleOnce(
            Duration.create(Delays.COORDINATOR_ACK_TIMEOUT, TimeUnit.MILLISECONDS),
            getSelf(),
            new CoordinatorAckReceivedMsg(msg),
            getContext().system().dispatcher(),
            nextNode
        );
    }

    /**
     * The new coordinator has sent the synchronization message, so the replicas
     * can update their epoch and writeIndex.
     */
    private void onSynchronizationMsg(SynchronizationMsg msg) {
        this.coordinatorIndex = this.replicas.indexOf(getSender());
        this.isCoordinator = (this.replicaID == this.coordinatorIndex);
        this.onCoordinatorChange();
        if (this.isCoordinator) { // Multicast sends to itself
            getContext().become(createCoordinator());
            // The new coordinator should start sending heartbeat messages, so
            // it sends itself a start message so that the appropriate timer is
            // set
            getSelf().tell(new StartMsg(), getSelf());
            return;
        }
        // Since no there's a new coordinator, the time of last contact must be
        // reset
        this.resetLastContact();
        System.out.printf(
            "[R%d] received synchronization message from %d\n",
            this.replicaID,
            this.coordinatorIndex
        );
    }

    /**
     * Send the synchronization message to all nodes.
     */
    public void sendSynchronizationMessage() {
        multicast(new SynchronizationMsg());
    }

    /**
     * The coordinator, which is the one with the most recent updates, sends all
     * the missed updates to each replica.
     */
    public void sendLostUpdates() {
        for (var entry : this.lastWriteForReplica.entrySet()) {
            var replica = this.replicas.get(entry.getKey());
            var lastUpdate = entry.getValue();
            var missedUpdatesList = new ArrayList<WriteMsg>();
            for (int i = lastUpdate.index + 1; i < this.lastWrite.index + 1; i++) {
                var writeId = new WriteId(this.epoch, i);
                var ithRequest = this.writeRequests.get(writeId);
                missedUpdatesList.add(new WriteMsg(null, writeId, ithRequest));
            }
            if (missedUpdatesList.isEmpty())
                continue;
            this.tellWithDelay(replica, new LostUpdatesMsg(missedUpdatesList));
        }
    }

    /**
     * The replica has received the lost updates from the coordinator, so it can
     * apply them.
     */
    private void onLostUpdatesMsg(LostUpdatesMsg msg) {
        System.out.printf(
            "[R%d] received %d missed updates, last update: (%d, %d), new updates received: %s\n",
            this.replicaID,
            msg.missedUpdates.size(),
            this.lastWrite.epoch,
            this.lastWrite.index,
            msg.missedUpdates.stream().map(
                update -> String.format(
                    "(%d, %d)",
                    update.id.epoch,
                    update.id.index
                )
            ).collect(Collectors.toList())
        );

        for (var update : msg.missedUpdates) {
            this.value = update.value;
        }

        // Exit the election state and go back to normal
        getContext().become(createReceive());
        // Now, all deferred update Requests can be safely served
        ActorRef coordinator = this.replicas.get(this.coordinatorIndex);
        for (UpdateRequestMsg req : this.deferredUpdateRequests) {
            this.tellWithDelay(coordinator, req, ActorRef.noSender());
            System.out.printf(
                "Replica %s sent deferred req with id %d\n",
                getSelf().path().name(),
                req.id.index
            );
        }
        this.deferredUpdateRequests = null;
    }

    private void onElectionAckMsg(ElectionAckMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(getSender(), msg.index);
        // The ACK has arrived, so remove it from the set of pending ones
        this.pendingElectionAcks.remove(pair);
    }

    private void onCoordinatorAckMsg(CoordinatorAckMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(getSender(), msg.index);
        // The ACK has arrived, so remove it from the set of pending ones
        this.pendingCoordinatorAcks.remove(pair);
    }

    //=== HANDLERS FOR CRASH DETECTION MESSAGES ================================
    /**
     * When a CrashMsg is received there's cercain chance of actually crashing.
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
            this.heartbeatTimer.cancel();

            System.out.printf(
                    "[R] Replica %s received crash message and CRASHED\n",
                    getSelf().path().name()
            );
        }
    }

    /**
     * When a HeartbeatMsg is received:
     * - Coordinator multicasts it to all replicas;
     * - Replicas reset they're time of last contact from the coordinator;
     */
    private void onHeartbeatMsg(HeartbeatMsg msg) {
        if (this.isCoordinator) {
            // Sends an heartbeat to all replicas signaling that it's still
            // alive.
            // The coordinator should not send it to itself otherwise it would
            // keeps sending them infinitely.
            this.multicast(msg, true);
        } else {
            // Since a replica has received a heartbeat it knows the coordinator
            // is still alive
            this.resetLastContact();
        }
    }

    /**
     * Timeout for the WriteMsg. If the pair is still in the map, the replica has not responded in time
     */
    private void onWriteMsgTimeoutReceivedMsg(WriteMsgReceivedMsg msg) {
        if (this.pendingUpdateRequests.contains(msg.updateRequestId)) {
            // No WriteMsg received, coordinator crashed
            this.recordCoordinatorCrash(
                String.format("missed WriteMsg for write req %d from %s",
                msg.updateRequestId.index,
                msg.updateRequestId.client.path().name()
            ));
        }
    }

    /**
     * Timeout for the WriteOk. If the pair is still in the map, the replica has not responded in time
     */
    private void onWriteOkTimeoutReceivedMsg(WriteOkReceivedMsg msg) {
        if (!this.writeOks.remove(msg.writeMsgId)) {
            // No WriteOk received, coordinator crashed
            this.recordCoordinatorCrash(
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
    private void onHeartbeatTimeoutReceivedMsg(HeartbeatReceivedMsg msg) {
        long now = new Date().getTime();
        long elapsed = now - this.lastContact;
        if (elapsed > Delays.RECEIVE_HEARTBEAT_TIMEOUT) {
            // Too much time has passed since last hearing from the coordinator
            // The coordinator in crashed
            this.recordCoordinatorCrash(
                String.format("missed HeartbeatMsg: %d elapsed",
                elapsed
            ));
        }
    }

    /**
     * When the timeout for the ACK on the election message is received, if the pair is still in the map
     * it means that the replica has not received the acknowledgement, and so we add it to the crashed replicas
     */
    private void onElectionAckTimeoutReceivedMsg(ElectionAckReceivedMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(getSender(), msg.msg.index);
        // If the pair is still in the set, the replica who should have sent the
        // ACK is probably crashed
        if (!this.pendingElectionAcks.contains(pair)) {
            return;
        }

        this.crashedReplicas.add(getSender());
        // The election message should be sent again
        ActorRef nextNode = getNextNode();
        if (nextNode == getSelf()) {
            // There's no other active replica, so this should become the
            // coordinator
            getSelf().tell(new SynchronizationMsg(), getSelf());
        } else {
            this.tellWithDelay(nextNode, msg);
        }
    }

    /**
     * When receiving the timeout for a coordinator acknowledgment, if the pair is still in the map,
     * it means that the other replica has crashed.
     */
    private void onCoordinatorAckTimeoutReceivedMsg(CoordinatorAckReceivedMsg msg) {
        var pair = new AbstractMap.SimpleEntry<>(getSender(), msg.msg.index);
        // If the pair is still in the set, the replica who should have sent the
        // ACK is probably crashed
        if (!this.pendingCoordinatorAcks.contains(pair)) {
            return;
        }

        // The coordinator message should be sent again
        ActorRef nextNode = getNextNode();
        if (nextNode == getSelf()) {
            // There's no other active replica, so this should become the
            // coordinator
            getSelf().tell(new SynchronizationMsg(), getSelf());
        } else {
            this.tellWithDelay(nextNode, msg);
        }
    }

    /**
     * Auxiliary method for resetting time of last contact with the coordinator.
     */
    private void resetLastContact() {
        this.lastContact = new Date().getTime();
    }

    /**
     * Auxiliary method for recording the crash of the coordinator.
     * It stops the timer for heartbeats and initiates the election protocol.
     */
    private void recordCoordinatorCrash(String cause) {
        this.crashedReplicas.add(
            this.replicas.get(this.coordinatorIndex)
        );

        // Stop sending heartbeat received messages
        this.heartbeatTimer.cancel();

        System.out.printf(
            "[R] Coordinator crash detected by replica %s on %s\n",
            getSelf().path().name(),
            cause
        );

        // Initiate election protocol
        this.beginElection();
        this.sendElectionMessage();
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
                .match(ElectionMsg.class, this::onElectionMsg)
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(HeartbeatReceivedMsg.class, this::onHeartbeatTimeoutReceivedMsg)
                .match(WriteMsgReceivedMsg.class, this::onWriteMsgTimeoutReceivedMsg)
                .match(WriteOkReceivedMsg.class, this::onWriteOkTimeoutReceivedMsg)
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
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(WriteAckMsg.class, this::onWriteAckMsg)
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
                .match(UpdateRequestMsg.class, this.electionBehaviour::onUpdateRequestMsg)
                .match(ElectionMsg.class, this::onElectionMsg)
                .match(CoordinatorMsg.class, this::onCoordinatorMsg)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
                .match(LostUpdatesMsg.class, this::onLostUpdatesMsg)
                .match(ElectionAckMsg.class, this::onElectionAckMsg)
                .match(CoordinatorAckMsg.class, this::onCoordinatorAckMsg)
                .match(ElectionAckReceivedMsg.class, this::onElectionAckTimeoutReceivedMsg)
                .match(CoordinatorAckReceivedMsg.class, this::onCoordinatorAckTimeoutReceivedMsg)
                .match(CrashMsg.class, this::onCrashMsg)
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

    public int getReplicaID() {
        return this.replicaID;
    }

    public void setCoordinatorIndex(int coordinatorIndex) {
        this.coordinatorIndex = coordinatorIndex;
    }

    public int getCoordinatorIndex() {
        return coordinatorIndex;
    }

    public void setLastWriteForReplica(Map<Integer, WriteId> lastWriteForReplica) {
        this.lastWriteForReplica = lastWriteForReplica;
    }

    public WriteId getLastWrite() {
        return lastWrite;
    }
}