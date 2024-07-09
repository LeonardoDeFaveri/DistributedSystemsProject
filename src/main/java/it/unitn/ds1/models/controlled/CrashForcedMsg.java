package it.unitn.ds1.models.controlled;

import java.io.Serializable;

import akka.actor.ActorRef;

/**
 * This message tells a replica to crash (mandatorily).
 */
public class CrashForcedMsg implements Serializable {
    public final ActorRef replica;

    public CrashForcedMsg(ActorRef replica) {
        this.replica = replica;
    }
}
