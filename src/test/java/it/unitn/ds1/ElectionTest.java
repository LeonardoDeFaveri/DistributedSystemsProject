package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.administratives.JoinGroupMsg;
import it.unitn.ds1.models.administratives.StartMsg;
import it.unitn.ds1.models.election.ElectionMsg;
import it.unitn.ds1.models.update.WriteMsg;
import it.unitn.ds1.models.update.WriteOkMsg;
import it.unitn.ds1.utils.UpdateRequestId;
import it.unitn.ds1.utils.WriteId;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class ElectionTest {
    @Test
    public void testElection() {
        final ActorSystem system = ActorSystem.create("electionTest");
        final int N_REPLICAS = 5;

        final ArrayList<ActorRef> replicas = new ArrayList<>();

        for (int i = 0; i < N_REPLICAS; i++) {
            // Initially the coordinator is the first created replica
            replicas.add(system.actorOf(Replica.props(i, 0, 0), "replica" + i));
        }

        for (ActorRef replica : replicas) {
            replica.tell(new JoinGroupMsg(replicas), ActorRef.noSender());
            replica.tell(new StartMsg(), ActorRef.noSender());
        }

        var client = system.actorOf(Client.props(replicas), "client");

        var values = new int[]{1, 2, 3, 4, 5};

        // First, let them add the messages to the list
        for (int i = 0; i < values.length; i++) {
            WriteId id = new WriteId(0, i);
            for (var replica : replicas) {
                replica.tell(new WriteMsg(new UpdateRequestId(client, i), id, values[i]), null);
            }
        }

        // Then, we send the WriteOk selectively

        // 0 has all the updates
        for (int i = 0; i < values.length; i++) {
            WriteId id = new WriteId(0, i);
            replicas.get(2).tell(new WriteMsg(new UpdateRequestId(client, i), id, values[i]), null);
            replicas.get(2).tell(new WriteOkMsg(new WriteId(0, i), new UpdateRequestId(client, i)), null);
        }

        // 1 and 2 have one less update
        for (int i = 0; i < values.length - 1; i++) {
            WriteId id = new WriteId(0, i);
            replicas.get(1).tell(new WriteMsg(new UpdateRequestId(client, i), id, values[i]), null);
            replicas.get(0).tell(new WriteMsg(new UpdateRequestId(client, i), id, values[i]), null);
            replicas.get(0).tell(new WriteOkMsg(new WriteId(0, i), new UpdateRequestId(client, i)), null);
            replicas.get(1).tell(new WriteOkMsg(new WriteId(0, i), new UpdateRequestId(client, i)), null);
        }
        // 3 and 4 have two less updates
        for (int i = 0; i < values.length - 2; i++) {
            WriteId id = new WriteId(0, i);
            replicas.get(3).tell(new WriteMsg(new UpdateRequestId(client, i), id, values[i]), null);
            replicas.get(4).tell(new WriteMsg(new UpdateRequestId(client, i), id, values[i]), null);
            replicas.get(4).tell(new WriteOkMsg(new WriteId(0, i), new UpdateRequestId(client, i)), null);
            replicas.get(3).tell(new WriteOkMsg(new WriteId(0, i), new UpdateRequestId(client, i)), null);
        }

        // Then, we start the election algorithm
        replicas.get(0).tell(new ElectionMsg(0, 0, 4, new WriteId(0, 2)), replicas.get(4));

        // Required to see all output
        while (replicas.size() > 0) {
        }

        System.out.printf(">>> Press ENTER <<<\n");
        try {
            System.in.read();
        } catch (IOException ioe) {
            System.err.println("IO error");
            System.err.println("Terminating system");
            system.terminate();
            System.exit(1);
        }
    }
}
