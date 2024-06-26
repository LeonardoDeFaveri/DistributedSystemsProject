package it.unitn.ds1.models;

import java.io.Serializable;

import akka.actor.ActorRef;
import it.unitn.ds1.utils.UpdateRequestId;

/**
 * Sent by the client to the replica, to update the value.
 */
public class UpdateRequestMsg implements Serializable {
    public final UpdateRequestId id;
    public final int value;

    public UpdateRequestMsg(ActorRef client, int value, int index) {
        this.value = value;
        this.id = new UpdateRequestId(client, index);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof UpdateRequestMsg)) {
            return false;
        }

        UpdateRequestMsg other = (UpdateRequestMsg) obj;
        return this.id.equals(other.id) && this.value == other.value;
    }

    @Override
    public int hashCode() {
        return String.format("%d-%d", this.id.hashCode(), this.value).hashCode();
    }
}