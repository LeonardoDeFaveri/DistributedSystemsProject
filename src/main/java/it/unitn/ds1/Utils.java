package it.unitn.ds1;

import java.util.Map;

import it.unitn.ds1.utils.WriteId;

public class Utils {

    /**
     * Get the new coordinator from a list of nodes, each with a WriteId object,
     * containing the last write applied by that node.
     * The node with the highest epoch is chosen. If there is a tie, the node
     * with the highest write index is chosen. If there is still a tie, the node
     * with the highest ID is chosen.
     *
     * @param current The node we are considering
     * @param highest The node with (temporarily) the newest update
     * @return The node with the newest update
     */
    public static Map.Entry<Integer, WriteId> getNewCoordinatorIndex(
            Map.Entry<Integer, WriteId> current,
            Map.Entry<Integer, WriteId> highest
        ) {
        // If current is later than highest, returns current since it's more
        // updated
        if (!current.getValue().isPriorOrEqualTo(highest.getValue())) {
            return current;
        }

        // If current and highest are equal return current if it has a higher ID
        if (
            current.getValue().equals(highest.getValue()) &&
            current.getKey() > highest.getKey()
        ) {
            return current;
        }

        // If none of the above, highest is still the most updated
        return highest;
    }
}
