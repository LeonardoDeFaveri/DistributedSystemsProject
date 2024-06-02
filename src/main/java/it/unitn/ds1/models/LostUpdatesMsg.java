package it.unitn.ds1.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LostUpdatesMsg implements Serializable {
    public List<WriteMsg> missedUpdates = new ArrayList<>();

    public LostUpdatesMsg(List<WriteMsg> missedUpdates) {
        this.missedUpdates = missedUpdates;
    }
}
