package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.*;

import java.io.IOException;
import java.util.ArrayList;

public class ElectionTest {

    //    @Test
//    public void testElection() {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("electionTest");
        final int N_REPLICAS = 5;

        final ArrayList<ActorRef> replicas = new ArrayList<>();

        for (int i = 0; i < N_REPLICAS; i++) {
            // Initially the coordinator is the first created replica
            replicas.add(system.actorOf(Replica.props(i, 0, -1), "replica" + i));
        }

        for (ActorRef replica : replicas) {
            replica.tell(new JoinGroupMsg(replicas), ActorRef.noSender());
        }

        var values = new int[]{1, 2, 3, 4, 5};

        // First, let them add the messages to the list
        for (int i = 0; i < values.length; i++) {
            for (var replica : replicas) {
                replica.tell(new WriteMsg(values[i], 0, i), null);
            }
        }

        // Then, we send the WriteOk selectively

        // 0 has all the updates
        for (int i = 0; i < values.length; i++) {
            replicas.get(2).tell(new WriteOkMsg(0, i), null);
        }

        // 1 and 2 have one less update
        for (int i = 0; i < values.length - 1; i++) {
            replicas.get(1).tell(new WriteOkMsg(0, i), null);
            replicas.get(0).tell(new WriteOkMsg(0, i), null);
        }
        // 3 and 4 have two less updates
        for (int i = 0; i < values.length - 2; i++ ){
            replicas.get(3).tell(new WriteOkMsg(0, i), null);
            replicas.get(4).tell(new WriteOkMsg(0, i), null);
        }

        // Then, we start the election algorithm
        replicas.get(0).tell(new ElectionMsg(4, new ElectionMsg.LastUpdate(0, 2)), replicas.get(4));

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
