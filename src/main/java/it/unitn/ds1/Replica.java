package it.unitn.ds1;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    private final List<ActorRef> replicas;
    private int coordinatorIndex;
    private boolean isCoordinator;

    private int v; // The value of the replica

    private int quorum = 0; // The number of nodes that must agree on a write
    private int epoch; // The current epoch
    private int writeIndex; // The index of the last write operation

    private final Map<Map.Entry<Integer, Integer>, Set<ActorRef>> writeAcksMap = new HashMap<>(); // The number of  write acks received for each write

    private final Map<Map.Entry<Integer, Integer>, Integer> writeRequests = new HashMap<>(); // The write requests the replica has received from the coordinator, the value is the new value to write

    public Replica(int v, int coordinatorIndex) {
        System.out.println("Replica created with value " + v);
        this.replicas = new ArrayList<>();
        this.coordinatorIndex = coordinatorIndex;
        this.isCoordinator = false;
        this.v = v;
    }

    public static Props props(int v, int coordinatorIndex) {
        return Props.create(Replica.class, () -> new Replica(v, coordinatorIndex));
    }

    // --------------------------------------------------------------------------

    private void onJoinGroupMsg(JoinGroupMsg msg) {
        for (ActorRef replica : msg.replicas) {
            this.replicas.add(replica);
        }
        this.quorum = (this.replicas.size() / 2) + 1;

        this.isCoordinator = this.replicas.indexOf(this.getSelf()) == this.coordinatorIndex;
        System.out.println();
    }

    private void multicast(Serializable msg) {
        for (ActorRef replica : this.replicas) {
            replica.tell(msg, this.self());
        }
    }

    private void onUpdateRequest(UpdateRequestMsg msg) {
        // If the replica is not the coordinator
        if (!this.isCoordinator) {
            // Send the request to the coordinator
            var coordinator = this.replicas.get(this.coordinatorIndex);
            coordinator.tell(msg, this.self());
            this.writeIndex++;
            return;
        }

        // The coordinator is requesting to itself
        if (getSender().path().uid() == this.getSelf().path().uid())
            return;

        // Implement the quorum protocol. The coordinator asks all the replicas to
        // update
        multicast(new WriteMsg(msg.v, this.epoch, this.writeIndex));
        this.writeIndex++;

        System.out.println("Client " + getSender().path().name() + " wr9te req to " + this.getSelf().path().name() + " for " + msg.v + " in epoch " + this.epoch + " with index " + this.writeIndex);
    }

    private void onWriteAckMsg(WriteAckMsg msg) {
        if (!this.isCoordinator)
            return;

        // If the epoch of the write is not the current epoch, ignore the message
        if (msg.epoch != this.epoch)
            return;

        // If the pair epoch-index is not in the map, add the list
        var pair = new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex);
        this.writeAcksMap.putIfAbsent(pair, new HashSet<>());

        // Add the sender to the list
        this.writeAcksMap.get(pair).add(getSender());

        // If the quorum has been reached, remove the pair from the map and send the
        // WriteOk message
        if (this.writeAcksMap.get(pair).size() >= this.quorum) {
            this.writeAcksMap.remove(pair);
            multicast(new WriteOkMsg(msg.epoch, msg.writeIndex));
        }

        System.out.println("Received ack from " + getSender().path().name() + " for " + msg.writeIndex + " in epoch " + msg.epoch);
    }

    /**
     * The coordinator is requesting to write a new value to the replicas
     */
    private void onWriteMsg(WriteMsg msg) {
        // Add the request to the list, so that it is ready if the coordinator requests
        // the update
        this.writeRequests.put(new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex), msg.v);
        // Send the acknowledgement to the coordinator
        getSender().tell(new WriteAckMsg(msg.epoch, msg.writeIndex), this.self());
        // Increase the write index

        System.out.println("Write requested by the coordinator " + getSender().path().name() + " to "
                + this.getSelf().path().name() + " for " + msg.v);
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

        System.out.println("Applied the write " + msg.writeIndex + " in epoch " + msg.epoch + " with value " + this.v);
    }

    /**
     * The client is requesting to read the value of the replica
     */
    private void onReadMsg(ReadMsg msg) {
        ActorRef sender = getSender();
        sender.tell(new ReadOkMsg(this.v), this.self());

        System.out.println("Client " + getSender().path().name() + " read req to " + this.getSelf().path().name());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(WriteAckMsg.class, this::onWriteAckMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .build();
    }

    /**
     * Create a new coordinator replica, similar to the other replicas, but can
     * handle updates
     * [This seems to be useless for now]
     */
    public Receive createCoordinator() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(WriteAckMsg.class, this::onWriteAckMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .build();
    }
}