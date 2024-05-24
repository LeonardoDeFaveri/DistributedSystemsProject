package it.unitn.ds1;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.models.ReadMsg;
import it.unitn.ds1.models.ReadOkMsg;
import it.unitn.ds1.models.UpdateRequestMsg;
import scala.concurrent.duration.Duration;

public class Client extends AbstractActor {
    // Maximun value to be generated for update messages
    static final int MAX_INT = 1000;

    private final ArrayList<ActorRef> replicas; // All replicas in the system
    private int v; // Last read value
    private int favoriteReplica;
    private Random numberGenerator;

    public Client(ArrayList<ActorRef> replicas) {
        this.replicas = replicas;
        this.v = 0;
        this.favoriteReplica = -1;
        this.numberGenerator = new Random(System.nanoTime());
    }

    public static Props props(ArrayList<ActorRef> replicas) {
        return Props.create(Client.class, () -> new Client(replicas));
    }

    @Override
    public void preStart() {
        // Create a timer that will periodically send READ messages to a replica
        Cancellable readTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS), // when to start generating messages
                Duration.create(1, TimeUnit.SECONDS), // how frequently generate them
                this.getReplica(), // destination actor reference
                new ReadMsg(), // the message to send
                getContext().system().dispatcher(), // system dispatcher
                getSelf() // source of the message (myself)
        );

        // Create a timer that will periodically send WRITE messages to a replica
        Cancellable writeTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS),
                Duration.create(1, TimeUnit.SECONDS),
                this.getReplica(),
                new UpdateRequestMsg(this.numberGenerator.nextInt(MAX_INT)),
                getContext().system().dispatcher(),
                getSelf());
    }

    /**
     * Return the replica to be contacted. If favourite replica is available, it
     * is returned, otherwise another random one is choosen and set as favourite.
     * @return replica to be contacted
     */
    private ActorRef getReplica() {
        if (this.favoriteReplica < 0) {
            this.favoriteReplica = this.numberGenerator.nextInt(this.replicas.size());
        }

        return this.replicas.get(this.favoriteReplica);
    }

    // --------------------------------------------------------------------------

    private void onReadOk(ReadOkMsg msg) {
        // Updates client value with the one read from a replica
        this.v = msg.v;
        System.out.println("Client " + getSelf().path().name() + " read done " + v);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadOkMsg.class, this::onReadOk)
                .build();
    }

}