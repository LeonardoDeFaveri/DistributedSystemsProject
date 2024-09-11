package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.administratives.JoinGroupMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.administratives.StopMsg;
import it.unitn.ds1.models.controlled.CrashForcedMsg;
import it.unitn.ds1.models.controlled.ReadForcedMsg;
import it.unitn.ds1.models.controlled.UpdateRequestForcedMsg;
import it.unitn.ds1.utils.KeyEvents;
import it.unitn.ds1.utils.ProgrammedCrash;

import java.io.IOException;
import java.util.ArrayList;

public class Broadcaster {
    final static int N_CLIENTS = 1;
    final static int N_REPLICAS = 4;

    public static void main(String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("project");
        final ArrayList<ActorRef> replicas = new ArrayList<>();

        int value = 0;
        // Initially the coordinator is the first created replica
        int coordinatorIndex = 0;
        for (int i = 0; i < N_REPLICAS; i++) {
            ProgrammedCrash schedule = new ProgrammedCrash();

            // Program crashes for each replica
            switch (i) {
                case 0:
                    schedule.program(KeyEvents.WRITE_ACK_ALL, false, 1);
                    break;
                case 1:
                    break;
                case 2:
                    schedule.program(KeyEvents.ELECTION_1, false, 1);
                    break;
                case 3:
                    schedule.program(KeyEvents.WRITE_OK, true, 1);
                    break;
                case 4:
                    break;
            }

            replicas.add(system.actorOf(
                Replica.props(
                    i,
                    value,
                    coordinatorIndex,
                    schedule
                ),
                "replica" + i
            ));
        }



        for (ActorRef replica : replicas) {
            replica.tell(new JoinGroupMsg(replicas), ActorRef.noSender());
            replica.tell(new StartMsg(), ActorRef.noSender());
        }
        var client = system.actorOf(Client.controlledProps(replicas), "client");
        
        sendUpdateRequest(client, replicas.get(1));

        Thread.sleep(5000);
        sendReadMsg(client, replicas.get(1));
        sendReadMsg(client, replicas.get(1));

        requestContinue(system, "terminate system");
        system.terminate();
        System.out.flush();
        System.out.println(">>> System terminated");
    }

    private static void normalFunctioning(ActorSystem system, ArrayList<ActorRef> replicas) {
        ArrayList<ActorRef> clients = new ArrayList<>();
        // Creates all the clients that will send UPDATE messages
        for (int i = 0; i < N_CLIENTS; i++) {
            clients.add(system.actorOf(Client.props(replicas), "client" + i));
        }
        // Creates the actor that will periodically make replicas crash
        //ActorRef crashManager = system.actorOf(CrashManager.props(replicas), "crash_manager");

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
        //crashManager.tell(new StartMsg(), ActorRef.noSender());

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
    }

    private static void requestContinue(ActorSystem system, String msg) {
        System.out.printf(">>> Press ENTER to %s <<<%n", msg);
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

    private static void makeReplicaCrash(ActorRef crashManager, ActorRef replica) {
        crashManager.tell(new CrashForcedMsg(replica), ActorRef.noSender());
    }

    private static void sendReadMsg(ActorRef client, ActorRef replica) {
        client.tell(new ReadForcedMsg(replica), ActorRef.noSender());
    }

    private static void sendUpdateRequest(ActorRef client, ActorRef replica) {
        client.tell(new UpdateRequestForcedMsg(replica), ActorRef.noSender());
    }
}
