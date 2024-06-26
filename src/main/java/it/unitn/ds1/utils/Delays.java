package it.unitn.ds1.utils;

/**
 * This interface collects definitions of delays and timeout times. 
 */
public interface Delays {
    /**
     * Maximum possible delay for sending of messages.
     */
    final int MAX_DELAY = 100;
    /**
     * Time to wait before checking for the receipt of a WriteOkMsg.
     */
    final long WRITEOK_TIMEOUT = 3000;
    /**
     * Time to wait before checking for the receipt of a writeMsg.
     */
    final long WRITEMSG_TIMEOUT = 3000;
    /**
     * Time to wait before sending a new HeartbeatMsg. Should be used by the
     * coordinator to demonstrate is liveness.
     */
    final long SEND_HEARTBEAT_TIMEOUT = 50;
    /**
     * Time to wait before sending a new HeartbeatMsg. Should be used by a
     * replica to check when was the last time it was contacted by the
     * coordinator.
     */
    final long RECEIVE_HEARTBEAT_TIMEOUT = 300;

    /**
     * Time to wait before checking for the receipt of a ReadOkMsg. Should be
     * used by clients to check for liveness of the replica contacted for a
     * read request.
     */
    final long READOK_TIMEOUT = 1000;

    /**
     * Time to wait before checking for the receipt of and ElectionAckMsg.
     */
    final long ELECTION_ACK_TIMEOUT = 1000;
    /**
     * Time to wait before checking for the receipt of a CoordinatorAckMsg.
     */
    final long COORDINATOR_ACK_TIMEOUT = 1000;

    /**
     * Time to wait before sending the first CrashMsg.
     */
    final long CRASH_WAIT = 1000;
    /**
     * How often are crash messages sent to replicas?
     */
    final long CRASH_FREQUENCY = 2000;
}
