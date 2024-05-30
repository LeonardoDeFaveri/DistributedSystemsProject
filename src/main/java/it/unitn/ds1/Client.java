package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.models.*;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {
    // Maximum value to be generated for update messages
    static final int MAX_INT = 1000;

    private final ArrayList<ActorRef> replicas; // All replicas in the system
    private final Random numberGenerator;
    private int v; // Last read value
    private int favoriteReplica;
    private Cancellable readTimer;
    private Cancellable writeTimer;

    public Client(ArrayList<ActorRef> replicas) {
        this.replicas = replicas;
        this.v = 0;
        this.favoriteReplica = -1;
        this.numberGenerator = new Random(System.nanoTime());

        System.out.printf("[C] Client %s created\n", getSelf().path().name());
    }

    public static Props props(ArrayList<ActorRef> replicas) {
        return Props.create(Client.class, () -> new Client(replicas));
    }

    /**
     * Return the replica to be contacted. If favorite replica is available, it
     * is returned, otherwise another random one is chosen and set as favorite.
     *
     * @return replica to be contacted
     */
    private ActorRef getReplica() {
        return this.replicas.get(this.numberGenerator.nextInt(this.replicas.size()));
//        if (this.favoriteReplica < 0) {
//            this.favoriteReplica = this.numberGenerator.nextInt(this.replicas.size());
//        }
//
//        System.out.printf(
//                "[C] Client %s chose replica %s as favorite\n",
//                getSelf().path().name(),
//                this.replicas.get(this.favoriteReplica).path().name()
//        );
//        return this.replicas.get(this.favoriteReplica);
    }

    // --------------------------------------------------------------------------

    private void onStartMsg(StartMsg msg) {
        System.out.printf("[C] Client %s started\n", getSelf().path().name());

        // Create a timer that will periodically send READ messages to a replica
        this.readTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS), // when to start generating messages
                Duration.create(10, TimeUnit.SECONDS), // how frequently generate them
                this.getReplica(), // destination actor reference
                new ReadMsg(), // the message to send
                getContext().system().dispatcher(), // system dispatcher
                getSelf() // source of the message (myself)
        );

        // Create a timer that will periodically send WRITE messages to a replica
        this.writeTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(4, TimeUnit.SECONDS),
                this.getReplica(),
                new UpdateRequestMsg(this.numberGenerator.nextInt(MAX_INT)),
                getContext().system().dispatcher(),
                getSelf());
    }

    private void onStopMsg(StopMsg msg) {
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

    private void onReadOk(ReadOkMsg msg) {
        // Updates client value with the one read from a replica
        this.v = msg.v;
        System.out.printf(
                "[C] Client %s read done %d\n",
                getSelf().path().name(),
                this.v
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMsg.class, this::onStartMsg)
                .match(StopMsg.class, this::onStopMsg)
                .match(ReadOkMsg.class, this::onReadOk)
                .build();
    }

}