package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.ElectionMsg;
import it.unitn.ds1.models.JoinGroupMsg;

import java.io.IOException;
import java.util.ArrayList;

public class ElectionTest {

//    @Test
//    public void testElection() {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("electionTest");
        final int N_REPLICAS = 5;

        final ArrayList<ActorRef> replicasActors = new ArrayList<>();

        for (int i = 0; i < N_REPLICAS; i++) {
            // Initially the coordinator is the first created replica
            replicasActors.add(system.actorOf(Replica.props(i, 0, -1), "replica" + i));
        }

        for (ActorRef replica : replicasActors) {
            replica.tell(new JoinGroupMsg(replicasActors), ActorRef.noSender());
        }

        replicasActors.get(0).tell(new Replica.SetManuallyMsg(1, 5), replicasActors.get(0));
        replicasActors.get(1).tell(new Replica.SetManuallyMsg(3, 1), replicasActors.get(0));
        replicasActors.get(2).tell(new Replica.SetManuallyMsg(2, 5), replicasActors.get(0));
        replicasActors.get(3).tell(new Replica.SetManuallyMsg(5, 3), replicasActors.get(3));
        replicasActors.get(4).tell(new Replica.SetManuallyMsg(2, 3), replicasActors.get(4));

        replicasActors.get(2).tell(new ElectionMsg(1, new ElectionMsg.LastUpdate(3, 1)), replicasActors.get(1));

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
