package it.unitn.ds1.models.election;

import java.io.Serializable;

public class CoordinatorAckMsg implements Serializable {
    public final int index;

    public CoordinatorAckMsg(int index) {
        this.index = index;
    }
}
