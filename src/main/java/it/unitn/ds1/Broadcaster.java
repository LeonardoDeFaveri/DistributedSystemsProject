package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.administratives.JoinGroupMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.administratives.StopMsg;

import java.io.IOException;
import java.util.ArrayList;

public class Broadcaster {
    final static int N_CLIENTS = 2;
    final static int N_REPLICAS = 4;

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("project");

        final ArrayList<ActorRef> replicas = new ArrayList<>();
        final ArrayList<ActorRef> clients = new ArrayList<>();

        for (int i = 0; i < N_REPLICAS; i++) {
            // Initially the coordinator is the first created replica
            replicas.add(system.actorOf(Replica.props(i, 0, 0), "replica" + i));
        }

        for (ActorRef replica : replicas) {
            replica.tell(new JoinGroupMsg(replicas), ActorRef.noSender());
        }

        // Creates all the clients that will send UPDATE messages
        for (int i = 0; i < N_CLIENTS; i++) {
            clients.add(system.actorOf(Client.props(replicas), "client" + i));
        }

        // Creates the actor that will periodically make replicas crash
        ActorRef crashManager = system.actorOf(CrashManager.props(replicas), "crash_manager");

        System.out.print(">>> System ready");
        requestContinue(system, "send start signal to all hosts");

        // Send StartMsg to replicas so that they begin sending HeartbeatMsg
        // (coordinator) and start their timer for heartbeatMsg (replicas)
        for (ActorRef replica : replicas) {
            replica.tell(new StartMsg(), ActorRef.noSender());
        }

        // Send StartMsg to clients so that they begin producing requests
        for (ActorRef client : clients) {
            client.tell(new StartMsg(), ActorRef.noSender());
        }
        crashManager.tell(new StartMsg(), ActorRef.noSender());

        System.out.print(">>> System working");
        requestContinue(system, "send stop signal to clients");
        // Send StopMsg to clients so that they stop producing requests
        // This allows replicas to terminate serving old requests so that one can
        // check whether the system is consistent after all requests have been
        // served
        for (ActorRef client : clients) {
            client.tell(new StopMsg(), ActorRef.noSender());
        }

        requestContinue(system, "terminate system");
        system.terminate();
        System.out.println(">>> System terminated");

        // System.out.println("Current java version is " + System.getProperty("java.version"));
    }

    private static void requestContinue(ActorSystem system, String msg) {
        System.out.printf(">>> Press ENTER to %s <<<\n", msg);
        try {
            // The loop is necessary to flush the System.in buffer
            do {
                System.in.read();
            } while (System.in.available() > 0);
        } catch (IOException ioe) {
            System.err.println("IO error");
            System.err.println("Terminating system");
            system.terminate();
            System.exit(1);
        }
    }
}
