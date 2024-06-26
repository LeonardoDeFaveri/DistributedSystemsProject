package it.unitn.ds1.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Logger {
    private static final String FILE_NAME = "log.txt";

    public static void logUpdate(int replicaID, int epoch, int writeIndex, int value) {
        log(String.format("Replica %s update %d:%d %d%n", replicaID, epoch, writeIndex, value));
    }

    public static void logRead(int clientID, String replicaID) {
        log(String.format("Client %d read req to %s%n", clientID, replicaID));
    }

    public static void logReadDone(int clientID, int value) {
        log(String.format("Client %d read done %d%n", clientID, value));
    }

    private static void log(String message) {
        try {
            Files.write(Paths.get(FILE_NAME), message.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
