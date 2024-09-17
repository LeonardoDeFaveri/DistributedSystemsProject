package it.unitn.ds1.utils;

/**
 * This interface collects definitions of delays and timeout times. 
 */
public interface Delays {
    /**
     * Maximum possible delay for sending a message.
     */
    int MAX_DELAY = 100;
    /**
     * Time to wait before checking for the receipt of a WriteOkMsg.
     */
    long WRITEOK_TIMEOUT = 3000;
    /**
     * Time to wait before checking for the receipt of a writeMsg.
     */
    long WRITEMSG_TIMEOUT = 3000;
    /**
     * Time to wait before sending a new HeartbeatMsg. Should be used by the
     * coordinator to demonstrate is liveness.
     */
    long SEND_HEARTBEAT_TIMEOUT = 50;
    /**
     * Time to wait before sending a new HeartbeatMsg. Should be used by a
     * replica to check when was the last time it was contacted by the
     * coordinator.
     */
    long RECEIVE_HEARTBEAT_TIMEOUT = 300;

    /**
     * Time to wait before checking for the receipt of a ReadOkMsg. Should be
     * used by clients to check for liveness of the replica contacted for a
     * read request.
     */
    long READOK_TIMEOUT = 1000;
    /**
     * Time to wait before checking for the receipt of an UpdateRequestOkMsg.
     * Should be used to check for liveness of the replica contacted
     * for an update read request.
     */
    long UPDATE_REQUEST_OK_TIMEOUT = 1000;

    /**
     * Time to wait before checking for the receipt of and ElectionAckMsg.
     */
    long ELECTION_ACK_TIMEOUT = 500;
    /**
     * Time to wait before checking for the receipt of a CoordinatorAckMsg.
     */
    long COORDINATOR_ACK_TIMEOUT = 1000;

    /**
     * Time to wait before sending the first CrashMsg.
     */
    long CRASH_WAIT = 1000;
    /**
     * How often are crash messages sent to replicas?
     */
    long CRASH_FREQUENCY = 2000;
}
