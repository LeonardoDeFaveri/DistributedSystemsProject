package it.unitn.ds1.utils;

public class ElectionId {
    /**
     * ID of the replica who started this election.
     */
    public final Integer initiator;
    /**
     * Epoch that will be closed when this election completes.
     */
    public final Integer epoch;
    /**
     * Each election message does two rounds. In the first one each replica
     * adds itself to the list of participants. In the second each of them
     * checks if it's become the new coordinator.
     */
    public final Boolean isFirstRound;

    public ElectionId(int initiator, int epoch, boolean isFirstRound) {
        this.initiator = initiator;
        this.epoch = epoch;
        this.isFirstRound = isFirstRound;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ElectionId)) {
            return false;
        }

        ElectionId other = (ElectionId) obj;
        return this.initiator == other.initiator &&
            this.epoch == other.epoch &&
            this.isFirstRound == other.isFirstRound;
    }

    @Override
    public int hashCode() {
        return String.format(
            "%d-%d-%d",
            this.initiator.hashCode(),
            this.epoch.hashCode(),
            this.isFirstRound.hashCode()
        ).hashCode();
    }
}
