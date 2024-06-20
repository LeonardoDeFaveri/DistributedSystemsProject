package it.unitn.ds1.models.election;

import java.io.Serializable;

public class ElectionAckMsg implements Serializable {
    public final int index;

    public ElectionAckMsg(int index) {
        this.index = index;
    }
}
