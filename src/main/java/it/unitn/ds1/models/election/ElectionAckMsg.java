package it.unitn.ds1.models.election;

import java.io.Serializable;

import it.unitn.ds1.utils.ElectionId;

public class ElectionAckMsg implements Serializable {
    public final ElectionId id;

    public ElectionAckMsg(ElectionId id) {
        this.id = id;
    }
}
