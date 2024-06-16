package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.crash_detection.CrashMsg;
import it.unitn.ds1.models.crash_detection.CrashResponseMsg;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class CrashManager extends AbstractActor {
    private final ArrayList<ActorRef> replicas;
    private final int quorum;
    private final Random numberGenerator;
    private Cancellable crashTimer;

    public CrashManager(List<ActorRef> replicas) {
        this.replicas = new ArrayList<>();
        this.replicas.addAll(replicas);
        this.quorum = (this.replicas.size() / 2) + 1;
        this.numberGenerator = new Random(System.nanoTime());

        System.out.printf(
                "[CM] Crash manager %s created with a quorum of %d\n",
                getSelf().path().name(),
                this.quorum
        );
    }

    public static Props props(List<ActorRef> replicas) {
        return Props.create(CrashManager.class, () -> new CrashManager(replicas));
    }

    /**
     * Returns a random replica.
     */
    private ActorRef getReplica() {
        int index = this.numberGenerator.nextInt(this.replicas.size());
        return this.replicas.get(index);
    }

    private void onStartMsg(StartMsg msg) {
        System.out.println("[CM] CrashManager started");

        if (this.replicas.size() > this.quorum) {
            // Periodically sends a crash message to self and then redirect it
            // to a random replica
            this.crashTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.create(1, TimeUnit.SECONDS), // when to start generating messages
                    Duration.create(2, TimeUnit.SECONDS), // how frequently generate them
                    getSelf(), // destination actor reference
                    new CrashMsg(), // the message to send
                    getContext().system().dispatcher(), // system dispatcher
                    getSelf() // source of the message (myself)
            );
        }
    }

    /**
     * For a crash manager, a crash msg received tells the manager to send
     * that message to a random replica.
     */
    private void onCrashMsg(CrashMsg msg) {
        ActorRef replica = this.getReplica();

        System.out.printf(
                "[CM] CrashManager sent crash message to %s\n",
                replica.path().name()
        );
        replica.tell(msg, getSelf());
    }

    private void onCrashResponseMsg(CrashResponseMsg msg) {
        // The contacted replica hasn't crashed
        if (!msg.isCrashed) {
            return;
        }

        // Register the replica as crashed
        this.replicas.remove(getSender());

        // No more replica can crash
        if (replicas.size() <= quorum) {
            this.crashTimer.cancel();
            this.crashTimer = null;
            System.out.println("[CM] Quorum reached, no more crash message will be sent");
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMsg.class, this::onStartMsg)
                .match(CrashMsg.class, this::onCrashMsg)
                .match(CrashResponseMsg.class, this::onCrashResponseMsg)
                .build();
    }

}
