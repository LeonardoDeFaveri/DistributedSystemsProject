package it.unitn.ds1.models.controlled;

import akka.actor.ActorRef;

/**
 * Forces a client to send a ReadMsg to a certain replica.
 */
public class ReadForcedMsg {
    public final ActorRef replica;

    public ReadForcedMsg(ActorRef replica) {
        this.replica = replica;
    }
}
