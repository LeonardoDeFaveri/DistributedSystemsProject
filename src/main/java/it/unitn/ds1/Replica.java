package it.unitn.ds1;

import java.io.Serializable;
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
import it.unitn.ds1.models.crash_detection.*;
import it.unitn.ds1.utils.UpdateRequestId;
import it.unitn.ds1.utils.WriteId;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.stream.Collectors;

public class Replica extends AbstractActor {
    static final int CRASH_CHANCES = 100;
    static final int DELAY = 100;
    static final long WRITEOK_TIMEOUT = 3000;
    static final long WRITEMSG_TIMEOUT = 3000;
    static final long SEND_HEARTBEAT_TIMEOUT = 50;
    static final long RECEIVE_HEARTBEAT_TIMEOUT = 300;

    private final List<ActorRef> replicas; // List of all replicas in the system
    private final Set<ActorRef> crashedReplicas; // Set of crashed replicas
    private final Random numberGenerator;
    private final int replicaID;
    private int coordinatorIndex; // Index of the coordinator replica inside `replicas`
    private boolean isCoordinator;
  
    private int value; // The value of the replica
    private int quorum = 0; // The number of nodes that must agree on a write
    private int epoch; // The current epoch
    private int writeIndex; // The index of the last write operation

    // The last write operation, to prevent an old write to be applied after a new one
    private WriteId lastWrite = new WriteId(0, -1);

    private final Set<UpdateRequestId> pendingUpdateRequests;

    // The number of write acks received for each write
    private final Map<WriteId, Set<ActorRef>> writeAcksMap;

    // The write requests the replica has received from the coordinator, the value is the new value to write
    private final Map<WriteId, Integer> writeRequests;
    
    // Maps each write request to the updateRequest that initiated it
    private final Map<WriteId, UpdateRequestId> writesToUpdates;

    // Uses <clientRef, updateIndex> to keep track for those update request received
    // by a client that will need to be ACKed back
    private final Set<UpdateRequestId> updateRequests;

    private int currentWriteToAck = 0; // The write we are currently collecting ACKs for.

    // For the coordinator: periodically sends heartbeat messages to
    // replicas
    // For replicas: periodically sends heartbeat received messages
    // to themselves
    private Cancellable heartbeatTimer;
    private long lastContact;

    // The last update received from each replica. The key is the replicaID, the value is the last update (epoch, writeIndex)
    private Map<Integer, ElectionMsg.LastUpdate> lastUpdateForReplica = new HashMap<>();
    private ElectionMsg.LastUpdate lastUpdateApplied; // Last write applied by the replica

    public Replica(int replicaID, int value, int coordinatorIndex) {
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), value);
        this.replicas = new ArrayList<>();
        this.crashedReplicas = new HashSet<>();
        this.coordinatorIndex = coordinatorIndex;
        this.isCoordinator = false;
        this.value = value;
        this.writeAcksMap = new HashMap<>();
        this.writeRequests = new HashMap<>();
        this.writesToUpdates = new HashMap<>();
        this.updateRequests = new HashSet<>();
        this.pendingUpdateRequests = new HashSet<>();
        this.lastUpdateApplied = new ElectionMsg.LastUpdate(-1, -1);
        this.replicaID = replicaID;

        this.numberGenerator = new Random(System.nanoTime());
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), value);
    }

    public static Props props(int replicaID, int v, int coordinatorIndex) {
        return Props.create(Replica.class, () -> new Replica(replicaID, v, coordinatorIndex));
    }

    /**
     * Sends `msg` to `receiver` with a random delay of `[0, DELAY)`ms.
     * @param receiver receiver of the message
     * @param msg message to send
     */
    private void tellWithDelay(ActorRef receiver, Serializable msg) {
        int delay = this.numberGenerator.nextInt(0, DELAY);
        getContext().system().scheduler().scheduleOnce(
            Duration.create(delay, TimeUnit.MILLISECONDS),
            receiver,
            msg,
            getContext().system().dispatcher(),
            getSelf()
        );
    }

    // -------------------------------------------------------------------------

    private void onJoinGroupMsg(JoinGroupMsg msg) {
        this.replicas.addAll(msg.replicas);
        this.quorum = (this.replicas.size() / 2); // ! No + 1, because one is itself

        this.isCoordinator = this.replicas.indexOf(this.getSelf()) == this.coordinatorIndex;
        if (this.isCoordinator) {
            getContext().become(createCoordinator());
        }
    }

    private void onStartMsg(StartMsg msg) {
        if (this.isCoordinator) {
            // Begin sending heartbeat messages to replicas
            this.heartbeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(0, TimeUnit.SECONDS), // when to start generating messages
                Duration.create(SEND_HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS), // how frequently generate them
                getSelf(), // All replicas
                new HeartbeatMsg(), // the message to send
                getContext().system().dispatcher(), // system dispatcher
                getSelf() // source of the message (myself)
            );
            System.out.printf("[Co] Coordinator %s started\n", getSelf().path().name());
        } else {
            // Begins sending heartbeat 
            this.heartbeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(RECEIVE_HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new HearbeatReceivedMsg(),
                getContext().system().dispatcher(),
                getSelf()
            );
            this.resetLastContact();
            System.out.printf("[R] Replica %s started\n", getSelf().path().name());
        }
    }
  
    
    ///**
    // * Overload of sendDelayed, with a random delay up to 100ms
    // */
    //private void sendDelayed(Serializable msg, ActorRef receiver) {
    //    int delay = this.numberGenerator.nextInt(100);
    //    sendDelayed(msg, receiver, delay);
    //}
    //
    ///**
    // * Send a delayed message to a replica
    // * @param msg The message to send
    // * @param receiver The replica to send the message to
    // * @param delay The delay in milliseconds
    // */
    //private void sendDelayed(Serializable msg, ActorRef receiver, int delay) {
    //    getContext().system().scheduler().scheduleOnce(
    //            Duration.create(delay, TimeUnit.MILLISECONDS), // delay
    //            receiver, // Receiver
    //            msg, // Message to send
    //            getContext().system().dispatcher(), // Executor
    //            getSelf() // Sender
    //    );
    //}

    private void multicast(Serializable msg) {
        multicast(msg, false);
    }

    /**
     * Multicasts a message to all replicas
     * @param msg The message to send
     * @param excludeItself Whether the replica should exclude itself from the multicast
     */
    private void multicast(Serializable msg, boolean excludeItself) {
        var replicas = this.replicas.stream().filter(r -> !excludeItself || r != this.getSelf()).toList();

        for (ActorRef replica : replicas) {
            //sendDelayed(msg, replica);
            this.tellWithDelay(replica, msg);
        }
    }

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
                Duration.create(WRITEMSG_TIMEOUT, TimeUnit.MILLISECONDS),
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

        // The OK has already been sent, as the quorum was reached. Ignore the message from other replicas
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
     * The first message to be served is the `currentWriteToAck` index.
     * When the message is sent to the replicas, serve all the successive messages
     */
    private void sendAllAckedMessages() {
        // Starting from the first message to send, if the quorum has been reached, send the message
        // Then, go to the next message (continue until the last write has been reached)
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
        // Add the request to the list, so that it is ready if the coordinator requests
        // the update
        this.writeRequests.put(msg.id, msg.v);
        // Send the acknowledgement to the coordinator
        this.tellWithDelay(
            getSender(),
            new WriteAckMsg(msg.id)
        );

        System.out.printf(
            "[R] Write requested by the coordinator %s to %s for %d in epoch %d and index %d\n",
            getSender().path().name(),
            this.getSelf().path().name(),
            msg.v,
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
            Duration.create(WRITEOK_TIMEOUT, TimeUnit.MILLISECONDS),
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

        int value = this.writeRequests.remove(msg.id);

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

        this.value = value;         // Apply the write
        this.lastWrite = msg.id;    // Update the last write
        // Update the last write
        this.lastUpdateApplied = new ElectionMsg.LastUpdate(msg.id.epoch, msg.id.index);
      
        System.out.printf(
            "[R] [%s] Applied the write %d in epoch %d with value %d\n",
            this.self().path().name(),
            msg.id.index,
            msg.id.epoch,
            this.value
        );

        this.resetLastContact();
    }

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

    /**
     * Election behaviour
     */

    private void onCoordinatorChange() {
        this.epoch++;
        this.writeIndex = 0;
    }

    /**
     * Returns the next node on the ring (the next based on index may have crashed, check!)
     *
     * @return The next node on the ring
     */
    private ActorRef getNextNode() {
        int currentIndex = this.replicas.indexOf(getSelf());
        int nextIndex = (currentIndex + 1) % this.replicas.size();
        return this.replicas.get(nextIndex);
    }

    /**
     * Send an election message to the next node
     */
    public void sendElectionMessage() {
        var nextNode = this.getNextNode();
        var electionMsg = new ElectionMsg(this.replicaID, this.lastUpdateApplied);
        nextNode.tell(electionMsg, this.getSelf());
    }

    /**
     * When an Election message is received:
     * - If the message already contains this replicaID, then change the type to Coordinator, and set the coordinatorID
     * to the node which is the most updated in the list (highest epoch and writeIndex), and take the node with the
     * highest ID in case of a tie
     * - Otherwise, add the replicaID of this node + the last update to the list, then propagate to the next node
     *
     * @param msg The message received
     */
    private void onElectionMsg(ElectionMsg msg) {
        System.out.println("[R:" + this.replicaID + "] election message received from replica " +
                getSender().path().name() + " with content: " +
                msg.participants.entrySet().stream()
                        .map((content) ->
                                String.format("{ replicaID: %d, lastUpdate: (%d, %d) }",
                                        content.getKey(), content.getValue().epoch, content.getValue().writeIndex)
                        ).collect(Collectors.joining(", "))
        );
        // When a node receives the election message, and the message already contains the node's ID,
        // then change the message type to COORDINATOR.
        // THe new leader is the node with the latest update (highest epoch, writeIndex), and highest replicaID
        if (msg.participants.containsKey(this.replicaID)) {
            var mostUpdated = msg.participants.entrySet().stream().reduce(Utils::getNewCoordinatorIndex);
            this.coordinatorIndex = mostUpdated.get().getKey();

            System.out.println("[R:" + this.replicaID + "] New coordinator found: " + this.coordinatorIndex);
            if (this.coordinatorIndex == this.replicaID) {
                this.lastUpdateForReplica = msg.participants;
                sendSynchronizationMessage();
                sendLostUpdates();
                this.onCoordinatorChange();
                return;
            }
            getNextNode().tell(new CoordinatorMsg(this.coordinatorIndex, this.replicaID, msg.participants), getSelf());
            return;
        }
        // If it's an election message, and my ID is not in the list, add it and propagate to the next node
        // writeIndex - 1 because we are incrementing the update after we receive it
        msg.participants.put(this.replicaID, this.lastUpdateApplied);

        ActorRef nextNode = getNextNode();
        nextNode.tell(msg, this.getSelf());
    }

    /**
     * The Election message has been received by all the nodes, and the coordinator has been elected.
     * The coordinator sends a message to all the nodes to synchronize the epoch and the writeIndex
     */
    private void onCoordinatorMsg(CoordinatorMsg msg) {
        // The replica is the sender of the message, it already has the coordinator index
        if (msg.senderID == this.replicaID)
            return;
        if (msg.coordinatorID == this.replicaID) {
            this.lastUpdateForReplica = msg.participants;
            sendSynchronizationMessage();
            sendLostUpdates();
            this.onCoordinatorChange();
            return;
        }
        System.out.printf("[R%d] received new coordinator %d from %d%n",
                this.replicaID, msg.coordinatorID, msg.senderID);
        this.coordinatorIndex = msg.coordinatorID; // Set the new coordinator
        getNextNode().tell(msg, getSelf()); // Forward the message to the next node
    }

    /**
     * The new coordinator has sent the synchronization message, so the replicas can update their epoch and writeIndex
     */
    private void onSynchronizationMsg(SynchronizationMsg msg) {
        this.coordinatorIndex = this.replicas.indexOf(getSender());
        this.isCoordinator = (this.replicaID == this.coordinatorIndex);
        if (this.isCoordinator) { // Multicast sends to itself
            getContext().become(createCoordinator());
            return;
        }
        this.onCoordinatorChange();
        System.out.printf("[R%d] received synchronization message from %d\n", this.replicaID, this.coordinatorIndex);
    }

    /**
     * Send the synchronization message to all nodes
     */
    private void sendSynchronizationMessage() {
        multicast(new SynchronizationMsg());
    }

    /**
     * The coordinator, which is the one with the most recent updates, sends all the missed updates to each replica.
     */
    private void sendLostUpdates() {
        for (var entry : this.lastUpdateForReplica.entrySet()) {
            var replica = this.replicas.get(entry.getKey());
            var lastUpdate = entry.getValue();
            var missedUpdatesList = new ArrayList<WriteMsg>();
            for (int i = lastUpdate.writeIndex + 1; i < this.lastUpdateApplied.writeIndex + 1; i++) {
                var writeId = new WriteId(this.epoch, i);
                var ithRequest = this.writeRequests.get(writeId);
                missedUpdatesList.add(new WriteMsg(null, writeId, ithRequest));
                //missedUpdatesList.add(new WriteMsg(ithRequest, this.epoch, i));
            }
            if (missedUpdatesList.isEmpty())
                continue;
            this.tellWithDelay(replica, new LostUpdatesMsg(missedUpdatesList));
        }
    }

    /**
     * The replica has received the lost updates from the coordinator, so it can apply them
     */
    private void onLostUpdatesMsg(LostUpdatesMsg msg) {
        System.out.printf("[R%d] received %d missed updates, last update: (%d, %d), new updates received: %s\n",
                this.replicaID, msg.missedUpdates.size(), lastUpdateApplied.epoch, lastUpdateApplied.writeIndex,
                msg.missedUpdates.stream().map(update -> String.format("(%d, %d)", update.id.epoch, update.id.index)).collect(Collectors.toList()));
        for (var update : msg.missedUpdates) {
            this.value = update.v;
        }
    }

    /**
     * Crash
     */

    private void onHeartbeatMsg(HeartbeatMsg msg) {
        if (this.isCoordinator) {
            // Sends an heartbeat to all other replicas
            this.multicast(msg);
        } else {
            // Reset lastContact
            this.resetLastContact();
        }
    }

    private void onWriteMsgReceivedMsg(WriteMsgReceivedMsg msg) {
        if (
            //!this.writeRequests.containsKey(msg.writeMsgId) &&
            //// Checks if last performed write is prior to checked write
            //// If this check is false it means that the request has already
            //// been served, thus a WriteMsg was received
            //this.lastWrite.isPriorOrEqualTo(msg.writeMsgId)
            this.pendingUpdateRequests.contains(msg.updateRequestId)
        ) {
            // No WriteMsg received, coordinator crashed
            this.recordCoordinatorCrash(
                String.format("missed WriteMsg for write req %d from %s",
                msg.updateRequestId.index,
                msg.updateRequestId.client.path().name()
            ));

            // TODO: initiate election protocol
        }
    }

    private void onWriteOkReceivedMsg(WriteOkReceivedMsg msg) {
        if (this.writeRequests.containsKey(msg.writeMsgId)) {
            // No WriteOk received, coordinator crashed
            this.recordCoordinatorCrash(
                String.format("missed WriteOk for epoch %d index %d",
                msg.writeMsgId.epoch,
                msg.writeMsgId.index
            ));

            // TODO: initiate election protocol
        }
    }

    /**
     * Checks how much time has elapsed since the last message received
     * from the coordinator. If too much has elapsed, then a crash is detected.
     */
    private void onHeartbetReceivedMsg(HearbeatReceivedMsg msg) {
        long now = new Date().getTime();
        long elapsed = now - this.lastContact;
        if (elapsed > RECEIVE_HEARTBEAT_TIMEOUT) {
            // Too much time has passed since last hearing from the coordinator
            // The coordinator in crashed
            this.recordCoordinatorCrash(
                String.format("missed HeartbeatMsg: %d elapsed",
                elapsed
            ));

            // TODO: initiate election protocol
        }
    }

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
     * Auxiliary method for resetting time of last contact with the coordinator.
     */
    private void resetLastContact() {
        this.lastContact = new Date().getTime();
    }

    /**
     * Auxiliary method for recording the crash of the coordinator.
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
    }

    /**
     * Message listeners
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
                .match(CoordinatorMsg.class, this::onCoordinatorMsg)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
                .match(LostUpdatesMsg.class, this::onLostUpdatesMsg)
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(HearbeatReceivedMsg.class, this::onHeartbetReceivedMsg)
                .match(WriteMsgReceivedMsg.class, this::onWriteMsgReceivedMsg)
                .match(WriteOkReceivedMsg.class, this::onWriteOkReceivedMsg)
                .match(CrashMsg.class, this::onCrashMsg)
                .build();
    }

    /**
     * Create a new coordinator replica, similar to the other replicas, but can
     * handle updates
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

    // Creates a crashed replica that doesn't handle any more message.
    final AbstractActor.Receive createCrashed() {
        return receiveBuilder()
                .build();
    }
}