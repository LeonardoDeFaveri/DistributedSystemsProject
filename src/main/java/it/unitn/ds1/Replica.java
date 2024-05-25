package it.unitn.ds1;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.models.JoinGroupMsg;
import it.unitn.ds1.models.ReadMsg;
import it.unitn.ds1.models.ReadOkMsg;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.WriteAckMsg;
import it.unitn.ds1.models.WriteMsg;
import it.unitn.ds1.models.WriteOkMsg;

public class Replica extends AbstractActor {
    private final List<ActorRef> replicas; // List of all replicas in the system
    private int coordinatorIndex; // Index of coordinator replica inside `replicas`
    private boolean isCoordinator;

    private int v; // The value of the replica

    private int quorum = 0; // The number of nodes that must agree on a write
    private int epoch; // The current epoch
    private int writeIndex; // The index of the last write operation

    // The number of  write acks received for each write
    private final Map<Map.Entry<Integer, Integer>, Set<ActorRef>> writeAcksMap = new HashMap<>();

    // The write requests the replica has received from the coordinator, the value is the new value to write
    private final Map<Map.Entry<Integer, Integer>, Integer> writeRequests = new HashMap<>();

    private int currentWriteToAck = 0; // The write we are currently collecting ACKs for.

    private Random numberGenerator;

    public Replica(int v, int coordinatorIndex) {
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), v);
        this.replicas = new ArrayList<>();
        this.coordinatorIndex = coordinatorIndex;
        this.isCoordinator = false;
        this.v = v;

        this.numberGenerator = new Random(System.nanoTime());
    }

    public static Props props(int v, int coordinatorIndex) {
        return Props.create(Replica.class, () -> new Replica(v, coordinatorIndex));
    }

    /**
     * Makes the process sleep for a random amount of time so as to simulate a
     * delay.
     */
    private void simulateDelay() {
        try { Thread.sleep(this.numberGenerator.nextInt(100)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    // -------------------------------------------------------------------------

    private void onJoinGroupMsg(JoinGroupMsg msg) {
        for (ActorRef replica : msg.replicas) {
            this.replicas.add(replica);
        }
        this.quorum = (this.replicas.size() / 2) + 1;

        this.isCoordinator = this.replicas.indexOf(this.getSelf()) == this.coordinatorIndex;
        if (this.isCoordinator) {
            //getContext().become(createCoordinator());
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

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                // There's no need for a replica to handle WriteAckMsg
                .match(WriteAckMsg.class, this::onWriteAckMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
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
                .build();
    }

    // Creates a crashed replica that doesn't handle any more message.
    final AbstractActor.Receive createCrashed() {
        return receiveBuilder()
                .build();
    }

    // TODO: implement behavior
    private void onCoordinatorChange() {
        this.epoch++;
        this.writeIndex = 0;
        this.currentWriteToAck = 0;
    }
}