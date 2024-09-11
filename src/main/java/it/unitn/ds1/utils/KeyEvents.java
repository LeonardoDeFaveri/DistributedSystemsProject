package it.unitn.ds1.utils;

/**
 * These are all the key events in the system.
 */
public enum KeyEvents {
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
     * Receipt of a WriteOk for a normal replica.
     */
    WRITE_OK,
    /**
     * Sending of a new election message which initiates a new election.
     */
    ELECTION_0,
    /** 
     * Receipt of an election message for the first time, starting the election
     * protocol
     */
    ELECTION_1,
    /** 
     * Receipt of a second-round election message.
     */
    ELECTION_2,
    /**
     * Receipt of an ElectionAck.
     */
    ELECTION_ACK,
    /**
     * This replica has become the new coordinator.
     */
    BECOME_COORDINATOR,
    /**
     * Receipt of a Synchronization message
     */
    SYNCHRONIZATION,
}
