package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.models.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Replica extends AbstractActor {
    private final List<ActorRef> replicas; // List of all replicas in the system
    // The number of  write acks received for each write
    private final Map<Map.Entry<Integer, Integer>, Set<ActorRef>> writeAcksMap = new HashMap<>();
    // The write requests the replica has received from the coordinator, the value is the new value to write
    private final Map<Map.Entry<Integer, Integer>, Integer> writeRequests = new HashMap<>();
    private final Random numberGenerator;
    private final int replicaID;
    private int coordinatorIndex; // Index of coordinator replica inside `replicas`
    private boolean isCoordinator;
    private int v; // The value of the replica
    private int quorum = 0; // The number of nodes that must agree on a write
    private int epoch; // The current epoch
    private int writeIndex; // The index of the last write operation
    private int currentWriteToAck = 0; // The write we are currently collecting ACKs for.

    public Replica(int replicaID, int v, int coordinatorIndex) {
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), v);
        this.replicas = new ArrayList<>();
        this.coordinatorIndex = coordinatorIndex;
        this.isCoordinator = false;
        this.v = v;
        this.replicaID = replicaID;

        this.numberGenerator = new Random(System.nanoTime());
    }

    public static Props props(int replicaID, int v, int coordinatorIndex) {
        return Props.create(Replica.class, () -> new Replica(replicaID, v, coordinatorIndex));
    }

    private static Map.Entry<Integer, ElectionMsg.LastUpdate> getNewCoordinatorIndex(
            Map.Entry<Integer, ElectionMsg.LastUpdate> current, Map.Entry<Integer, ElectionMsg.LastUpdate> highest) {
        if (current.getValue().epoch > highest.getValue().epoch) {
            // If the epoch of the current node is higher than the other, this node is the most updated
            return current;
        } else if (current.getValue().epoch == highest.getValue().epoch &&
                current.getValue().writeIndex > highest.getValue().writeIndex) {
            // If the epoch is the same, but the write index is higher, this node is the most updated
            return current;
        } else if (current.getValue().epoch == highest.getValue().epoch &&
                current.getValue().writeIndex == highest.getValue().writeIndex &&
                current.getKey() > highest.getKey()) {
            // If both the epoch and the write index are the same, but this has an higher ID, we choose this one
            // This is because we need a global rule on what node to choose in case of a tie
            return current;
        }
        // Otherwise, we already have the (temporary) most updated node
        return highest;
    }

    /**
     * Makes the process sleep for a random amount of time so as to simulate a
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
        this.quorum = (this.replicas.size() / 2) + 1;

        this.isCoordinator = this.replicas.indexOf(this.getSelf()) == this.coordinatorIndex;
        if (this.isCoordinator) {
            getContext().become(createCoordinator());
        }
    }

    private void multicast(Serializable msg) {
        for (ActorRef replica : this.replicas) {
            this.simulateDelay();
            replica.tell(msg, this.self());
        }
    }

    private void onUpdateRequest(UpdateRequestMsg msg) {
        // If the replica is not the coordinator
        if (!this.isCoordinator) {
            // Send the request to the coordinator
            var coordinator = this.replicas.get(this.coordinatorIndex);
            this.simulateDelay();
            coordinator.tell(msg, this.self());
            this.writeIndex++;
            return;
        }

        // Implement the quorum protocol. The coordinator asks all the replicas
        // to update
        multicast(new WriteMsg(msg.v, this.epoch, this.writeIndex));

        // Add the new write request to the map, so that the acks can be received
        var pair = new AbstractMap.SimpleEntry<>(this.epoch, this.writeIndex);
        this.writeAcksMap.putIfAbsent(pair, new HashSet<>());

        System.out.printf(
                "[C] Client %s write req to %s for %d in epoch %d with index %d\n",
                getSender().path().name(),
                getSelf().path().name(),
                msg.v,
                this.epoch,
                this.writeIndex
        );
        this.writeIndex++;
    }

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
        while (this.currentWriteToAck < this.writeIndex) {
            var pair = new AbstractMap.SimpleEntry<>(this.epoch, this.currentWriteToAck);
            if (this.writeAcksMap.containsKey(pair) && this.writeAcksMap.get(pair).size() >= this.quorum) {
                multicast(new WriteOkMsg(this.epoch, this.currentWriteToAck));
                this.writeAcksMap.remove(pair);
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
        // Add the request to the list, so that it is ready if the coordinator requests
        // the update
        this.writeRequests.put(new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex), msg.v);
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

        // Apply the write
        this.v = this.writeRequests.get(pair);
        this.writeRequests.remove(pair);

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
     * Message listeners
     */

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                // There's no need for a replica to handle WriteAckMsg
                //.match(WriteAckMsg.class, this::onWriteAckMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .match(ElectionMsg.class, this::onElectionMsg)
                .match(CoordinatorMsg.class, this::onCoordinatorMsg)
                .match(SetManuallyMsg.class, this::manualChangeMsg)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
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
                .match(SetManuallyMsg.class, this::manualChangeMsg)
                .build();
    }

    // Creates a crashed replica that doesn't handle any more message.
    final AbstractActor.Receive createCrashed() {
        return receiveBuilder()
                .build();
    }

    /**
     * Election behaviour
     */

    // TODO: implement behavior
    private void onCoordinatorChange() {
        this.epoch++;
        this.writeIndex = 0;
        this.currentWriteToAck = 0;
    }

    /**
     * Returns the next node on the ring (the next based on index may have crashed, check!)
     * TODO: implement the crash check
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
     * TODO: should the last write index based on the updates applied?
     */
    public void sendElectionMessage() {
        var nextNode = this.getNextNode();
        var electionMsg = new ElectionMsg(this.replicaID, new ElectionMsg.LastUpdate(this.epoch, this.writeIndex));
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
            var mostUpdated = msg.participants.entrySet().stream().reduce(Replica::getNewCoordinatorIndex);
            this.coordinatorIndex = mostUpdated.get().getKey();

            System.out.println("[R:" + this.replicaID + "] New coordinator found: " + this.coordinatorIndex);
            this.onCoordinatorChange();
            if (this.coordinatorIndex == this.replicaID) {
                sendSynchronizationMessage();
                return;
            }
            getNextNode().tell(new CoordinatorMsg(this.coordinatorIndex, this.replicaID), getSelf());
            return;
        }
        // If it's an election message, and my ID is not in the list, add it and propagate to the next node
        // writeIndex - 1 because we are incrementing the update after we receive it
        // TODO: should we use the latest write which has been APPLIED? YES
        msg.participants.put(this.replicaID, new ElectionMsg.LastUpdate(this.epoch, this.writeIndex - 1));

        ActorRef nextNode = getNextNode();
        nextNode.tell(msg, this.getSelf());
    }

    private void onCoordinatorMsg(CoordinatorMsg msg) {
        // The replica is the sender of the message, it already has the coordinator index
        if (msg.senderID == this.replicaID)
            return;
        if (msg.coordinatorID == this.replicaID) {
            sendSynchronizationMessage();
            return;
        }
        System.out.printf("[R%d] received new coordinator %d from %d%n",
                this.replicaID, msg.coordinatorID, msg.senderID);
        this.coordinatorIndex = msg.coordinatorID; // Set the new coordinator
        getNextNode().tell(msg, getSelf()); // Forward the message to the next node
    }

    private void onSynchronizationMsg(SynchronizationMsg msg) {
        this.coordinatorIndex = this.replicas.indexOf(getSender());
        System.out.printf("[R%d] received synchronization message from %d\n", this.replicaID, this.coordinatorIndex);
    }

    private void sendSynchronizationMessage() {
        multicast(new SynchronizationMsg());
    }

    public void manualChangeMsg(SetManuallyMsg msg) {
        this.epoch = msg.epoch;
        this.writeIndex = msg.writeIndex;
    }

    public record SetManuallyMsg(int epoch, int writeIndex) implements Serializable {
    }
}