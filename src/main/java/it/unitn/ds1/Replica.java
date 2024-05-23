package it.unitn.ds1;

import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.models.JoinGroupMsg;
import it.unitn.ds1.models.ReadMsg;
import it.unitn.ds1.models.ReadOkMsg;
import it.unitn.ds1.models.UpdateRequestMsg;

public class Replica extends AbstractActor {
    private final List<ActorRef> replicas;
    private int coordinatorIndex;
    private boolean isCoordinator;
    private int v;
    private int quorum = 0;

    public Replica(int v, int coordinatorIndex) {
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

    private void onUpdateRequest(UpdateRequestMsg msg) {
        // If the replica is not the coordinator
        if (!this.isCoordinator) {
            // Send the request to the coordinator
            var coordinator = this.replicas.get(this.coordinatorIndex);
            coordinator.tell(msg, this.self());
            return;
        }

        // Implement the quorum protocol. The coordinator asks all the replicas if
        System.out.println("Client " + getSender().path().name() + " wr9te req to " + this.getSelf().path().name()
                + " for " + msg.v);
    }

    private void onReadMsg(ReadMsg msg) {
        ActorRef sender = getSender();
        sender.tell(new ReadOkMsg(this.v), this.self());

        System.out.println("Client " + getSender().path().name() + " read req to " + this.getSelf().path().name());
    }

    @Override
    public Receive createReceive() {
        if (this.isCoordinator)
            return createCoordinator();
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(ReadMsg.class, this::onReadMsg)
                .build();
    }

    /**
     * Create a new coordinator replica, similar to the other replicas, but can
     * handle updates
     */
    public Receive createCoordinator() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequest)
                .match(ReadMsg.class, this::onReadMsg)
                .build();
    }
}