package it.unitn.ds1.utils;

import java.util.Objects;

/**
 * Class for a simpler handling of write identifiers
 */
public class WriteId {
    public final Integer epoch;
    public final Integer index;

    public WriteId(Integer epoch, Integer index) {
        this.epoch = epoch;
        this.index = index;
    }

    /**
     * Checks if this id is precedent or equal to other
     */
    public boolean isPriorOrEqualTo(WriteId other) {
        return this.epoch < other.epoch || Objects.equals(this.epoch, other.epoch) && this.index <= other.index;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof WriteId other)) {
            return false;
        }

        return Objects.equals(this.epoch, other.epoch) && Objects.equals(this.index, other.index);
    }

    @Override
    public int hashCode() {
        return String.format("%d-%d", this.epoch, this.index).hashCode();
    }
}
