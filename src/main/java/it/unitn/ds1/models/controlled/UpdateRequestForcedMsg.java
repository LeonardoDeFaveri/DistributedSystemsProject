package it.unitn.ds1.models.controlled;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Forces a client to send an UpdateRequestMsg to a certain replica.
 */
public class UpdateRequestForcedMsg implements Serializable {
    public final ActorRef replica;

    public UpdateRequestForcedMsg(ActorRef replica) {
        this.replica = replica;
    }
}