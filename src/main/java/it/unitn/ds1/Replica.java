package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.models.ReadMsg;
import it.unitn.ds1.models.ReadOkMsg;
import it.unitn.ds1.models.WriteMsg;

public class Replica extends AbstractActor {
    private final List<ActorRef> replicas;
    private int coordinatorIndex;
    private boolean isCoordinator;
    private int v;

    public Replica(int v, int coordinatorIndex) {
        this.replicas = new ArrayList<>();
        this.coordinatorIndex = coordinatorIndex;
        this.isCoordinator = false;
        this.v = v;
    }

    public static Props props(int v, int coordinatorIndex) {
        return Props.create(Replica.class, () -> new Replica(v, coordinatorIndex));
    }

    //--------------------------------------------------------------------------
    public static class JoinGroupMsg implements Serializable {
        // List of all replicas in the system
        public final List<ActorRef> replicas;

        public JoinGroupMsg(ArrayList<ActorRef> group) {
            this.replicas = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    //--------------------------------------------------------------------------

    private void onJoinGroupMsg(JoinGroupMsg msg) {
        for (ActorRef replica : msg.replicas) {
            this.replicas.add(replica);
        }

        this.isCoordinator = this.replicas.indexOf(this.getSelf()) == this.coordinatorIndex;
        System.out.println();
    }

    private void onWriteMsg(WriteMsg msg) {
        // TODO: implement UPDATE protocol

        System.out.println("Client " + getSender().path().name() + " wr9te req to " + this.getSelf().path().name() + " for " + msg.v);
    }

    private void onReadMsg(ReadMsg msg) {
        ActorRef sender = getSender();
        sender.tell(new ReadOkMsg(this.v), this.self());

        System.out.println("Client " + getSender().path().name() + " read req to " + this.getSelf().path().name());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
            .match(WriteMsg.class, this::onWriteMsg)
            .match(ReadMsg.class, this::onReadMsg)
            .build();
    }

    /**
     * Create a new coordinator replica, similar to the other replicas, but can handle updates
     */
    public Receive createCoordinator() {
        return receiveBuilder()
            .match(JoinGroupMsg.class,  this::onJoinGroupMsg)
            .match(WriteMsg.class, this::onWriteMsg)
            .match(ReadMsg.class, this::onReadMsg)
            .build();
    }
}