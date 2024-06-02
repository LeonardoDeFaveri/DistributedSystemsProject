package it.unitn.ds1.models;

import java.io.Serializable;

public class UpdateRequestOkMsg implements Serializable {
    public final int id;

    public UpdateRequestOkMsg(int id) {
        this.id = id;
    }
}
