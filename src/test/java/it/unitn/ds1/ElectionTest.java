package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.administratives.JoinGroupMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.controlled.CrashForcedMsg;
import it.unitn.ds1.models.controlled.ReadForcedMsg;
import it.unitn.ds1.models.controlled.UpdateRequestForcedMsg;
import it.unitn.ds1.models.update.WriteMsg;
import it.unitn.ds1.models.update.WriteOkMsg;
import it.unitn.ds1.utils.ProgrammedCrash;
import it.unitn.ds1.utils.UpdateRequestId;
import it.unitn.ds1.utils.WriteId;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class ElectionTest {
    @Test
    public void testElection() throws InterruptedException {
        final ActorSystem system = ActorSystem.create("electionTest");
        final int N_REPLICAS = 7;

        final ArrayList<ActorRef> replicas = new ArrayList<>();
        int value = 0;
        // Initially the coordinator is the first created replica
        int coordinatorIndex = 0;

        for (int i = 0; i < N_REPLICAS; i++) {
            ProgrammedCrash schedule = new ProgrammedCrash();

            // Program crashes for each replica
            switch (i) {
                case 0:
                    break;
                case 1:
                    //schedule.program(KeyEvents.ELECTION_1, false, 1);
                    break;
                case 2:
                    //schedule.program(KeyEvents.ELECTION_1, true, 1);
                    break;
                case 3:
                    break;
                case 4:
                    break;
            }

            replicas.add(system.actorOf(Replica.props(
                i,
                value,
                coordinatorIndex,
                schedule
            ), "replica" + i));
        }

        for (ActorRef replica : replicas) {
            replica.tell(new JoinGroupMsg(replicas), ActorRef.noSender());
            replica.tell(new StartMsg(), ActorRef.noSender());
        }

        var client = system.actorOf(Client.controlledProps(replicas), "client");
        ActorRef crashManager = system.actorOf(CrashManager.controlledProps(replicas), "crash_manager");

        var values = new int[]{1, 2, 3, 4, 5};

        // First, let them add the messages to the list
        for (int i = 0; i < values.length; i++) {
            WriteId id = new WriteId(0, i);
            for (var replica : replicas) {
                replica.tell(new WriteMsg(new UpdateRequestId(client, i), id, values[i]), null);
            }
        }

        // Then, we send the WriteOk selectively
        // 2 has all the updates
        for (int i = 0; i < values.length; i++) {
            WriteId id = new WriteId(0, i);
            UpdateRequestId uid = new UpdateRequestId(client, i);
            replicas.get(2).tell(new WriteMsg(uid, id, values[i]), null);
            replicas.get(2).tell(new WriteOkMsg(new WriteId(0, i), uid), null);
        }

        // 0 and 1 have one less update
        for (int i = 0; i < values.length - 1; i++) {
            WriteId id = new WriteId(0, i);
            UpdateRequestId uid = new UpdateRequestId(client, i);
            replicas.get(1).tell(new WriteMsg(uid, id, values[i]), null);
            replicas.get(0).tell(new WriteMsg(uid, id, values[i]), null);
            replicas.get(0).tell(new WriteOkMsg(new WriteId(0, i), uid), null);
            replicas.get(1).tell(new WriteOkMsg(new WriteId(0, i), uid), null);
        }
        // 3 and 4 have two less updates
        for (int i = 0; i < values.length - 2; i++) {
            WriteId id = new WriteId(0, i);
            UpdateRequestId uid = new UpdateRequestId(client, i);
            replicas.get(3).tell(new WriteMsg(uid, id, values[i]), null);
            replicas.get(4).tell(new WriteMsg(uid, id, values[i]), null);
            replicas.get(4).tell(new WriteOkMsg(new WriteId(0, i), uid), null);
            replicas.get(3).tell(new WriteOkMsg(new WriteId(0, i), uid), null);
        }

        // Then, we start the election algorithm
        makeReplicaCrash(crashManager, replicas.get(0));
        
        Thread.sleep(5000);
        sendReadMsg(client, replicas.get(1));
        sendReadMsg(client, replicas.get(2));
        sendReadMsg(client, replicas.get(3));
        sendReadMsg(client, replicas.get(4));

        sendUpdateRequest(client, replicas.get(4));
        Thread.sleep(2000);
        sendReadMsg(client, replicas.get(1));
        sendReadMsg(client, replicas.get(2));
        sendReadMsg(client, replicas.get(3));
        sendReadMsg(client, replicas.get(4));        

        // Required to see all output
        while (replicas.size() > 0) {}

        System.out.printf(">>> Press ENTER <<<%n");
        try {
            System.in.read();
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
