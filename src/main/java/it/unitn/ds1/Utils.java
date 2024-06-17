package it.unitn.ds1;

import java.util.Map;

import it.unitn.ds1.models.election.ElectionMsg;
import it.unitn.ds1.utils.WriteId;

public class Utils {

    /**
     * Get the new coordinator from a list of nodes, each with a LastUpdate object, containing the last write applied
     * by that node.
     * The node with the highest epoch is chosen. If there is a tie, the node with the highest write index is chosen.
     * If there is still a tie, the node with the highest ID is chosen.
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

        //if (current.getValue().epoch > highest.getValue().epoch) {
        //    // If the epoch of the current node is higher than the other, this node is the most updated
        //    return current;
        //} else if (current.getValue().epoch == highest.getValue().epoch &&
        //        current.getValue().index > highest.getValue().writeIndex) {
        //    // If the epoch is the same, but the write index is higher, this node is the most updated
        //    return current;
        //} else if (current.getValue().epoch == highest.getValue().epoch &&
        //        current.getValue().writeIndex == highest.getValue().writeIndex &&
        //        current.getKey() > highest.getKey()) {
        //    // If both the epoch and the write index are the same, but this has an higher ID, we choose this one
        //    // This is because we need a global rule on what node to choose in case of a tie
        //    return current;
        //}
        // Otherwise, we already have the (temporary) most updated node
        //return highest;
    }
}
