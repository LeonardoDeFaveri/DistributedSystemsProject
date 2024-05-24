package it.unitn.ds1.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import akka.actor.ActorRef;

/**
 * Sent by the system upon replicas creation to all replicas so that they all
 * know each other.
 */
public class JoinGroupMsg implements Serializable {
    // List of all replicas in the system
    public final List<ActorRef> replicas;

    public JoinGroupMsg(ArrayList<ActorRef> group) {
        this.replicas = Collections.unmodifiableList(new ArrayList<>(group));
    }
}
