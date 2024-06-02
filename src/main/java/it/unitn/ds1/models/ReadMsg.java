package it.unitn.ds1.models;

import java.io.Serializable;

import akka.actor.ActorRef;

/**
 * Sent by the client to the replica, to read the value.
 */
public class ReadMsg implements Serializable {
    public final ActorRef client;
    public final int id;

    public ReadMsg(ActorRef client, int id) {
        this.client = client;
        this.id = id;
    }
}
