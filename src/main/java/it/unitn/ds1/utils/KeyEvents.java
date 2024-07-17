package it.unitn.ds1.utils;

/**
 * These are all the key events in the system.
 */
public enum KeyEvents {
    /**
     * Actor creation.
     */
    CREATION,
    /**
     * Receipt of a StartMsg.
     */
    START,
    /**
     * Receipt of a read request.
     */
    READ,
    /**
     * Receipt of an update request.
     */
    UPDATE,
    /**
     * Receipt of a WriteMsg.
     */
    WRITE_MSG,
    /**
     * Receipt of a WriteACK.
     */
    WRITE_ACK,
    /**
     * Receipt of all WriteAcks necessary to trigger a WriteOk.
     */
    WRITE_ACK_ALL,
    /**
     * Receipt of a WriteOk.
     */
    WRITE_OK,
    /** 
     * Receipt of an election message for the first time, starting the election
     * protocol.
     */
    ELECTION_1,
    /** 
     * Receipt of an election message for the second time.
     */
    ELECTION_2,
    /**
     * Receipt of an ElectionAck.
     */
    ELECTION_ACK,
    /**
     * Another replica has been choosen as coordinator.
     */
    COORDINATOR_CHOOSEN,
    /**
     * This replica has become the new coordinator.
     */
    COORDINATOR_BECOME,
    /**
     * Receipt of a Synchronization message
     */
    SYNCHRONIZATION,
}
