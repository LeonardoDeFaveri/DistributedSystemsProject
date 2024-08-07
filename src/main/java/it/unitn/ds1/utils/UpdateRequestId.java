package it.unitn.ds1.utils;

import akka.actor.ActorRef;

/**
 * Class for simpler handling of update requests identifiers.
 * An update request is identified by the client that produced it and the
 * local update index of that client.
 */
public class UpdateRequestId {
    // Client that produced the update request
    public final ActorRef client;
    // Identifier, local to the client, of the request
    public final int index;

    public UpdateRequestId(ActorRef client, int index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof UpdateRequestId other)) {
            return false;
        }

        if (this.client == null) {
            return this.client == other.client && this.index == other.index;
        }

        return this.client.equals(other.client) && this.index == other.index;
    }

    @Override
    public int hashCode() {
        if (this.client == null) {
            return String.format("0-%d", this.index).hashCode();
        }
        return String.format("%d-%d", this.client.hashCode(), this.index).hashCode();
    }
}
