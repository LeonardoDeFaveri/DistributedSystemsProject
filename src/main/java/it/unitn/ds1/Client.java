package it.unitn.ds1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import it.unitn.ds1.models.*;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.administratives.StopMsg;
import it.unitn.ds1.models.crash_detection.*;
import it.unitn.ds1.utils.Delays;
import scala.concurrent.duration.Duration;

public class Client extends AbstractActor {
    /**
     * Maximum value to be generated for update messages.
     */
    static final int MAX_INT = 1000;

    /**
     * All replicas in the system, whether they're active or not.
     */
    private final ArrayList<ActorRef> replicas;
    /**
     * Last value read from a replica.
     */
    private int value;

    /**
     * Index of last ReadMsg sent.
     */
    private int readIndex;
    /**
     * Index of last UpdateRequestMsg sent.
     */
    private int writeIndex;
    /**
     * Maps the index of each send ReadMsg to the replica it was sent to.
     */
    private final Map<Integer, ActorRef> readMsgs;
    /**
     * Maps the index of each send UpdateRequestMsg to the replica it was sent
     * to.
     */
    private final Map<Integer, ActorRef> writeMsgs;

    /**
     * A timer that periodically produces a ReadMsg.
     */
    private Cancellable readTimer;
    /**
     * A timer that periodically produces an UpdateRequestMsg with a random
     * value.
     */
    private Cancellable writeTimer;

    private final Random numberGenerator;

    public Client(ArrayList<ActorRef> replicas) {
        this.replicas = replicas;
        this.value = 0;
        this.readIndex = 0;
        this.writeIndex = 0;
        this.readMsgs = new HashMap<>();
        this.writeMsgs = new HashMap<>();
        this.numberGenerator = new Random(System.nanoTime());

        System.out.printf("[C] Client %s created\n", getSelf().path().name());
    }

    public static Props props(ArrayList<ActorRef> replicas) {
        return Props.create(Client.class, () -> new Client(replicas));
    }

    /**
     * Return the replica to be contacted. Is randomly chosen each time.
     * @return replica to be contacted
     */
    private ActorRef getRandomReplica() {
        int index = this.numberGenerator.nextInt(1, this.replicas.size());
        return this.replicas.get(index);
    }

    // -------------------------------------------------------------------------
    /**
     * When a StartMsg is received the client starts producing read and
     * update requests.
     */
    private void onStartMsg(@SuppressWarnings("unused") StartMsg msg) {
        System.out.printf("[C] Client %s started\n", getSelf().path().name());

        // Create a timer that will periodically send READ messages to a replica
        // and then the client will redirect the message to randomly chosen replica
        this.readTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS), // when to start generating messages
                Duration.create(10, TimeUnit.SECONDS), // how frequently generate them
                getSelf(), // destination actor reference
                new ReadMsg(null, 0), // the message to send
                getContext().system().dispatcher(), // system dispatcher
                getSelf() // source of the message (myself)
        );

        // Create a timer that will periodically send WRITE messages to self and
        // then the client will redirect the message to randomly chosen replica
        this.writeTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(4, TimeUnit.SECONDS),
                getSelf(),
                new UpdateRequestMsg(null, 0, 0),
                getContext().system().dispatcher(),
                getSelf());
    }

    /**
     * When a StopMsg is received the client stops producing new requests for
     * replicas.
     */
    private void onStopMsg(@SuppressWarnings("unused") StopMsg msg) {
        System.out.printf("[C] Client %s stopped\n", getSelf().path().name());
        if (this.readTimer != null) {
            this.readTimer.cancel();
            this.readTimer = null;
        }

        if (this.writeTimer != null) {
            this.writeTimer.cancel();
            this.writeTimer = null;
        }
    }

    private void onReadMsg(@SuppressWarnings("unused") ReadMsg msg) {
        ActorRef replica = this.getRandomReplica();
        ReadMsg readMessage = new ReadMsg(getSender(), this.readIndex++);
        this.readMsgs.putIfAbsent(readMessage.id, replica);
        replica.tell(readMessage, getSelf());

        getContext().system().scheduler().scheduleOnce(
            Duration.create(Delays.READOK_TIMEOUT, TimeUnit.MILLISECONDS),
            getSelf(),
            new ReadOkReceivedMsg(readMessage.id),
            getContext().system().dispatcher(),
            getSelf()
        );
    }

    private void onUpdateRequestMsg(@SuppressWarnings("unused") UpdateRequestMsg msg) {
        ActorRef replica = this.getRandomReplica();
        UpdateRequestMsg updateRequest = new UpdateRequestMsg(
            getSelf(),
            this.numberGenerator.nextInt(MAX_INT),
            this.writeIndex
        );
        this.writeMsgs.putIfAbsent(updateRequest.id.index, replica);
        replica.tell(updateRequest, getSelf());

        getContext().system().scheduler().scheduleOnce(
            Duration.create(Delays.UPDATE_REQUEST_OK_TIMEOUT, TimeUnit.MILLISECONDS),
            getSelf(),
            new UpdateRequestOkReceivedMsg(updateRequest.id.index),
            getContext().system().dispatcher(),
            getSelf()
        );

        System.out.printf(
            "[C] Client %s write req to %s for %d with index %d\n",
            getSelf().path().name(),
            replica.path().name(),
            updateRequest.value,
            this.writeIndex
        );

        this.writeIndex++;
    }

    private void onReadOk(ReadOkMsg msg) {
        // Updates client value with the one read from a replica
        this.value = msg.value;
        this.readMsgs.remove(msg.id);
        System.out.printf(
                "[C] Client %s read done %d\n",
                getSelf().path().name(),
                this.value
        );
    }

    private void onReadOkReceivedMsg(ReadOkReceivedMsg msg) {
        ActorRef replica = this.readMsgs.remove(msg.id);
        if (replica != null) {
            // The ReadOk msg has not been received, replica crashed
            // Remove replica from the set of active replicas
            this.replicas.remove(replica);

            System.out.printf(
                "[C] Client %s detected replica %s has crashed while waiting for an ACK on read\n",
                getSelf().path().name(),
                replica.path().name()
            );
        }
    }

    private void onUpdateRequestOkMsg(UpdateRequestOkMsg msg) {
        this.writeMsgs.remove(msg.id);
        System.out.printf(
            "[C] Client %s write done\n",
            getSelf().path().name()
        );
    }

    private void onUpdateRequestOkReceivedMsg(UpdateRequestOkReceivedMsg msg) {
        ActorRef replica = this.writeMsgs.remove(msg.index);
        if (replica != null) {
            // The UpdateRequestOk msg has not been received, replica crashed
            // Remove replica from the set of active replicas
            this.replicas.remove(replica);

            System.out.printf(
                "[C] Client %s detected replica %s has crashed while waiting for an ACK on update for write %d\n",
                getSelf().path().name(),
                replica.path().name(),
                msg.index
            );
        }
    }

    //=== SETUP OF MESSAGES HANDLERS ===========================================
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMsg.class, this::onStartMsg)
                .match(StopMsg.class, this::onStopMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadOkMsg.class, this::onReadOk)
                .match(ReadOkReceivedMsg.class, this::onReadOkReceivedMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequestMsg)
                .match(UpdateRequestOkMsg.class, this::onUpdateRequestOkMsg)
                .match(UpdateRequestOkReceivedMsg.class, this::onUpdateRequestOkReceivedMsg)
                .build();
    }

}