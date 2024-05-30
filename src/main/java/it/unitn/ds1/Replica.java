package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.models.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Replica extends AbstractActor {
    static final int CRASH_CHANCES = 100;

    private final List<ActorRef> replicas; // List of all replicas in the system
    private final Set<ActorRef> crashedReplicas; // Set of crashed replicas
    // The number of  write acks received for each write
    private final Map<Map.Entry<Integer, Integer>, Set<ActorRef>> writeAcksMap = new HashMap<>();
    // The write requests the replica has received from the coordinator, the value is the new value to write
    private final Map<Map.Entry<Integer, Integer>, Integer> writeRequests = new HashMap<>();
    private final Random numberGenerator;
    private final int replicaID;
    private int coordinatorIndex; // Index of the coordinator replica inside `replicas`
    private boolean isCoordinator;
    private int v; // The value of the replica
    private int quorum = 0; // The number of nodes that must agree on a write
    private int epoch; // The current epoch
    private int writeIndex; // The index of the last write operation

    // The last update received from each replica. The key is the replicaID, the value is the last update (epoch, writeIndex)
    private Map<Integer, ElectionMsg.LastUpdate> lastUpdateForReplica = new HashMap<>();
    private ElectionMsg.LastUpdate lastUpdateApplied; // Last write applied by the replica

    public Replica(int replicaID, int v, int coordinatorIndex) {
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), v);
        this.replicas = new ArrayList<>();
        this.crashedReplicas = new HashSet<>();
        this.coordinatorIndex = coordinatorIndex;
        this.isCoordinator = false;
        this.v = v;
        this.replicaID = replicaID;
        this.lastUpdateApplied = new ElectionMsg.LastUpdate(-1, -1);

        this.numberGenerator = new Random(System.nanoTime());
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), v);
    }

    public static Props props(int replicaID, int v, int coordinatorIndex) {
        return Props.create(Replica.class, () -> new Replica(replicaID, v, coordinatorIndex));
    }

    /**
     * Makes the process sleep for a random amount of time to simulate a
     * delay.
     */
    private void simulateDelay() {
        // The choice of the delay is highly relevant, to big delays slows down
        // too much the update protocol
        try {
            Thread.sleep(this.numberGenerator.nextInt(100));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    private void multicast(Serializable msg) {
        multicast(msg, false);
    }

    private void multicast(Serializable msg, boolean excludeItself) {
        var replicas = this.replicas.stream().filter(r -> !excludeItself || r != this.getSelf()).toList();
        for (ActorRef replica : replicas) {
            this.simulateDelay();
            replica.tell(msg, this.self());
        }
    }

    /**
     * When a client sends a request to update the value of the replica
     */
    private void onUpdateRequest(UpdateRequestMsg msg) {
        // If the replica is not the coordinator
        if (!this.isCoordinator) {
            // Send the request to the coordinator
            var coordinator = this.replicas.get(this.coordinatorIndex);
            this.simulateDelay();
            coordinator.tell(msg, this.self());
            // ! WARNING: I'm moving this to onWriteMsg, I think that we should increase it when we add the request to the list
            // this.writeIndex++;
            return;
        }

        // Add the new write request to the map, so that the acks can be received
        var pair = new AbstractMap.SimpleEntry<>(this.epoch, this.writeIndex);
        this.writeAcksMap.putIfAbsent(pair, new HashSet<>());

        // The coordinator asks all the replicas to update
        multicast(new WriteMsg(msg.v, this.epoch, this.writeIndex));

        this.writeIndex++;

        System.out.printf(
                "[C] Client %s write req to %s for %d in epoch %d with index %d\n",
                getSender().path().name(),
                getSelf().path().name(),
                msg.v,
                this.epoch,
                this.writeIndex
        );
    }

    /**
     * When the coordinator receives an ack from a replica
     */
    private void onWriteAckMsg(WriteAckMsg msg) {
        if (!this.isCoordinator)
            return;

        // If the epoch of the write is not the current epoch, ignore the message
        if (msg.epoch != this.epoch)
            return;

        var pair = new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex);

        // The OK has already been sent, as the quorum was reached. Ignore the message from other replicas
        if (!this.writeAcksMap.containsKey(pair))
            return;

        // Add the sender to the list
        this.writeAcksMap.get(pair).add(getSender());

        // Send all the messages that have been acked in FIFO order!
        sendAllAckedMessages();

        System.out.printf(
                "[Co] Received ack from %s for %d in epoch %d\n",
                getSender().path().name(),
                msg.writeIndex,
                msg.epoch
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
        int currentWriteToAck = this.lastUpdateApplied.writeIndex + 1;
        while (currentWriteToAck < this.writeIndex) {
            var pair = new AbstractMap.SimpleEntry<>(this.epoch, currentWriteToAck);
            if (this.writeAcksMap.containsKey(pair) && this.writeAcksMap.get(pair).size() >= this.quorum) {
                onWriteOkMsg(new WriteOkMsg(this.epoch, currentWriteToAck)); // Apply the write, then send the messages
                multicast(new WriteOkMsg(this.epoch, currentWriteToAck), true);
                currentWriteToAck++;
            } else {
                break;
            }
        }
    }

    /**
     * The coordinator is requesting to write a new value to the replicas
     */
    private void onWriteMsg(WriteMsg msg) {
        if (getSender() == this.getSelf()) // Multicast sends to itself
            return;
        // Add the request to the list, so that it is ready if the coordinator requests
        // the update
        this.writeRequests.put(new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex), msg.v);
        this.writeIndex++;
        // Send the acknowledgement to the coordinator
        this.simulateDelay();
        getSender().tell(new WriteAckMsg(msg.epoch, msg.writeIndex), this.self());

        System.out.printf(
                "[R] Write requested by the coordinator %s to %s for %d\n",
                getSender().path().name(),
                this.getSelf().path().name(),
                msg.v
        );
    }

    /**
     * The coordinator sent the write ok message, so the replicas can apply the
     * write
     */
    private void onWriteOkMsg(WriteOkMsg msg) {
        // If the epoch of the write is not the current epoch, ignore the message
        if (msg.epoch != this.epoch)
            return;

        // If the pair epoch-index is not in the map, ignore the message
        var pair = new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex);
        if (!this.writeRequests.containsKey(pair))
            return;

        // If the last write applied is newer than the current write, ignore the message
        // If we have already applied the write, ignore the message
        if (this.lastUpdateApplied.epoch > msg.epoch || (
                this.lastUpdateApplied.epoch == msg.epoch &&
                        (this.lastUpdateApplied.writeIndex > msg.writeIndex ||
                                this.lastUpdateApplied.writeIndex == msg.writeIndex)
        )) return;

        // Apply the write
        this.v = this.writeRequests.get(pair);
        // Update the last write
        this.lastUpdateApplied = new ElectionMsg.LastUpdate(msg.epoch, msg.writeIndex);

        System.out.printf(
                "[R] [%s] Applied the write %d in epoch %d with value %d\n",
                this.self().path().name(),
                msg.writeIndex,
                msg.epoch,
                this.v
        );
    }

    /**
     * The client is requesting to read the value of the replica
     */
    private void onReadMsg(ReadMsg msg) {
        ActorRef sender = getSender();
        this.simulateDelay();
        sender.tell(new ReadOkMsg(this.v), this.self());

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
                var ithRequest = this.writeRequests.get(new AbstractMap.SimpleEntry<>(this.epoch, i));
                missedUpdatesList.add(new WriteMsg(ithRequest, this.epoch, i));
            }
            if (missedUpdatesList.isEmpty())
                continue;
            this.simulateDelay();
            replica.tell(new LostUpdatesMsg(missedUpdatesList), getSelf());
        }
    }

    /**
     * The replica has received the lost updates from the coordinator, so it can apply them
     */
    private void onLostUpdatesMsg(LostUpdatesMsg msg) {
        System.out.printf("[R%d] received %d missed updates, last update: (%d, %d), new updates received: %s%n",
                this.replicaID, msg.missedUpdates.size(), lastUpdateApplied.epoch, lastUpdateApplied.writeIndex,
                msg.missedUpdates.stream().map(update -> String.format("(%d, %d)", update.epoch, update.writeIndex)).collect(Collectors.toList()));
        for (var update : msg.missedUpdates) {
            this.v = update.v;
        }
    }

    /**
     * Crash
     */

    private void onHeartbeatMsg(HeartbeatMsg msg) {

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

            System.out.printf(
                    "[R] Replica %s received crash message and CRASHED\n",
                    getSelf().path().name()
            );
        }
    }

    /**
     * Message listeners
     */

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .match(ElectionMsg.class, this::onElectionMsg)
                .match(CoordinatorMsg.class, this::onCoordinatorMsg)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
                .match(LostUpdatesMsg.class, this::onLostUpdatesMsg)
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
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
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
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