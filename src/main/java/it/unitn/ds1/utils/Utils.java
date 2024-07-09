package it.unitn.ds1.utils;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Utils {
    private static final Random NUMBER_GENERATOR = new Random(System.nanoTime());

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

    /**
     * Sends msg to receiver with a random delay of [0, DELAY)ms.
     *
     * @param receiver receiver of the message
     * @param msg      message to send
     */
    public static void tellWithDelay(ActorContext context, ActorRef sender, ActorRef receiver, Serializable msg) {
        int delay = NUMBER_GENERATOR.nextInt(0, Delays.MAX_DELAY);
        context.system().scheduler().scheduleOnce(
                Duration.create(delay, TimeUnit.MILLISECONDS),
                receiver,
                msg,
                context.system().dispatcher(),
                sender
        );
    }

    /**
     * Multicasts a message to all replicas, possibly excluding itself.
     *
     * @param msg           The message to send
     * @param excludeItself Whether the replica should exclude itself from
     *                      the multicast or not
     */
    public static void multicast(List<ActorRef> replicas, ActorContext context, ActorRef sender, Serializable msg, boolean excludeItself) {
        replicas = replicas.stream().filter(
                r -> !excludeItself || r != sender
        ).toList();

        for (ActorRef replica : replicas) {
            tellWithDelay(context, sender, replica, msg);
        }
    }
}
