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
import it.unitn.ds1.models.CrashMsg;
import it.unitn.ds1.models.CrashResponseMsg;
import it.unitn.ds1.models.JoinGroupMsg;
import it.unitn.ds1.models.ReadMsg;
import it.unitn.ds1.models.ReadOkMsg;
import it.unitn.ds1.models.StartMsg;
import it.unitn.ds1.models.UpdateRequestMsg;
import it.unitn.ds1.models.WriteAckMsg;
import it.unitn.ds1.models.WriteMsg;
import it.unitn.ds1.models.WriteOkMsg;
import it.unitn.ds1.models.crash_detection.HearbeatReceivedMsg;
import it.unitn.ds1.models.crash_detection.HeartbeatMsg;
import it.unitn.ds1.models.crash_detection.WriteMsgReceivedMsg;
import it.unitn.ds1.models.crash_detection.WriteOkReceivedMsg;
import scala.concurrent.duration.Duration;

public class Replica extends AbstractActor {
    static final int CRASH_CHANCES = 100;
    static final int DELAY = 100;
    static final long WRITEOK_TIMEOUT = 500;
    static final long SEND_HEARTBEAT_TIMEOUT = 50;
    static final long RECEIVE_HEARTBEAT_TIMEOUT = 300;

    private final List<ActorRef> replicas; // List of all replicas in the system
    private final Set<ActorRef> crashedReplicas; // Set of crashed replicas
    private int coordinatorIndex; // Index of the coordinator replica inside `replicas`
    private boolean isCoordinator;

    private int v; // The value of the replica

    private int quorum = 0; // The number of nodes that must agree on a write
    private int epoch; // The current epoch
    private int writeIndex; // The index of the last write operation

    // The last write operation, to prevent an old write to be applied after a new one
    private WriteMsg lastWrite = new WriteMsg(0, -1, -1);

    // The number of  write acks received for each write
    private final Map<Map.Entry<Integer, Integer>, Set<ActorRef>> writeAcksMap;

    // The write requests the replica has received from the coordinator, the value is the new value to write
    private final Map<Map.Entry<Integer, Integer>, Integer> writeRequests;

    private int currentWriteToAck = 0; // The write we are currently collecting ACKs for.

    private final Random numberGenerator; // Generator for delays

    // For the coordinator: periodically sends heartbeat messages to
    // replicas
    // For replicas: periodically sends heartbeat received messages
    // to themselves
    private Cancellable heartbeatTimer;
    private long lastContact;

    public Replica(int v, int coordinatorIndex) {
        this.replicas = new ArrayList<>();
        this.crashedReplicas = new HashSet<>();
        this.coordinatorIndex = coordinatorIndex;
        this.isCoordinator = false;
        this.v = v;
        this.writeAcksMap = new HashMap<>();
        this.writeRequests = new HashMap<>();

        this.numberGenerator = new Random(System.nanoTime());
        System.out.printf("[R] Replica %s created with value %d\n", getSelf().path().name(), v);
    }

    public static Props props(int v, int coordinatorIndex) {
        return Props.create(Replica.class, () -> new Replica(v, coordinatorIndex));
    }

    /**
     * Makes the process sleep for a random amount of time to simulate a
     * delay.
     */
    private void simulateDelay() {
        // The choice of the delay is highly relevant, to big delays slows down
        // too much the update protocol
        try { Thread.sleep(this.numberGenerator.nextInt(DELAY)); }
        catch (InterruptedException e) { e.printStackTrace(); }
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

    private void onStartMsg(StartMsg msg) {
        if (this.isCoordinator) {
            // Begin sending heartbeat messages to replicas
            this.heartbeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(0, TimeUnit.SECONDS), // when to start generating messages
                Duration.create(SEND_HEARTBEAT_TIMEOUT, TimeUnit.SECONDS), // how frequently generate them
                getSelf(), // All replicas
                new HeartbeatMsg(), // the message to send
                getContext().system().dispatcher(), // system dispatcher
                getSelf() // source of the message (myself)
            );
            System.out.printf("[Co] Coordinator %s started\n", getSelf().path().name());
        } else {
            // Begins sending heartbeat 
            this.heartbeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(0, TimeUnit.SECONDS),
                Duration.create(RECEIVE_HEARTBEAT_TIMEOUT, TimeUnit.SECONDS),
                getSelf(),
                new HearbeatReceivedMsg(),
                getContext().system().dispatcher(),
                getSelf()
            );
            this.resetLastContact();
            System.out.printf("[R] Replica %s started\n", getSelf().path().name());
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
        var pair = new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex);
        this.writeRequests.put(pair, msg.v);
        // Send the acknowledgement to the coordinator
        this.simulateDelay();
        getSender().tell(new WriteAckMsg(msg.epoch, msg.writeIndex), this.self());

        System.out.printf(
            "[R] Write requested by the coordinator %s to %s for %d\n",
            getSender().path().name(),
            this.getSelf().path().name(),
            msg.v
        );

        // The replicas sets a timeout for the expected WriteOk message from
        // the coordinator
        getContext().system().scheduler().scheduleOnce(
            Duration.create(WRITEOK_TIMEOUT, TimeUnit.MILLISECONDS),
            getSelf(),
            new WriteOkReceivedMsg(pair),
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
        if (msg.epoch != this.epoch)
            // Resetting last contact here would be wrong since the received
            // message comes from an already crashed coordinator
            return;

        // If the pair epoch-index is not in the map, ignore the message
        var pair = new AbstractMap.SimpleEntry<>(msg.epoch, msg.writeIndex);
        if (!this.writeRequests.containsKey(pair))
            return;

        int value = this.writeRequests.remove(pair);

        // If the last write applied is newer than the current write, ignore the message
        if (this.lastWrite.epoch > msg.epoch || (
                this.lastWrite.epoch == msg.epoch &&
                this.lastWrite.writeIndex > msg.writeIndex
            )) return;

        // Apply the write
        this.v = value;
        // Update the last write
        this.lastWrite = new WriteMsg(this.v, msg.epoch, msg.writeIndex);

        System.out.printf(
            "[R] [%s] Applied the write %d in epoch %d with value %d\n",
            this.self().path().name(),
            msg.writeIndex,
            msg.epoch,
            this.v
        );

        this.resetLastContact();
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
        if (!this.writeRequests.containsKey(msg.writeMsg)) {
            // No WriteMsg received, coordinator crashed
            this.recordCoordinatorCrash();

            // TODO: initiate election protocol
        }
    }

    private void onWriteOkReceivedMsg(WriteOkReceivedMsg msg) {
        if (this.writeRequests.containsKey(msg.writeMsg)) {
            // No WriteOk received, coordinator crashed
            this.recordCoordinatorCrash();

            // TODO: initiate election protocol
        }
    }

    /**
     * Checks how much time has elapsed since the last message received
     * from the coordinator. If too much has elapsed, then a crash is detected.
     */
    private void onHeartbetReceivedMsg(HearbeatReceivedMsg msg) {
        long now = new Date().getTime();
        if (now - this.lastContact > SEND_HEARTBEAT_TIMEOUT) {
            // Too much time has passed since last hearing from the coordinator
            // The coordinator in crashed
            this.recordCoordinatorCrash();

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
    private void recordCoordinatorCrash() {
        this.crashedReplicas.add(
            this.replicas.get(this.coordinatorIndex)
        );

        // Stop sending heartbeat received messages
        this.heartbeatTimer.cancel();

        System.out.printf(
            "[R] Coordinator crash detected by replica %s\n",
            getSelf().path().name()
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(StartMsg.class, this::onStartMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
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

    // TODO: implement behavior
    private void onCoordinatorChange() {
        this.epoch++;
        this.writeIndex = 0;
        this.currentWriteToAck = 0;
    }
}