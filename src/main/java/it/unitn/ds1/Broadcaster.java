package it.unitn.ds1;

import java.io.IOException;
import java.util.ArrayList;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.JoinGroupMsg;

public class Broadcaster {
  final static int N_CLIENTS = 5;
  final static int N_REPLICAS = 5;

  public static void main(String[] args) {
    final ActorSystem system = ActorSystem.create("project");

    final ArrayList<ActorRef> replicas = new ArrayList<>();
    for (int i = 0; i < N_REPLICAS; i++) {
      // Initially the coordinator is the first created replica
      replicas.add(system.actorOf(Replica.props(0, 0), "replica" + i));
    }

    for (ActorRef replica : replicas) {
      replica.tell(new JoinGroupMsg(replicas), ActorRef.noSender());
    }

    // Creates all the clients that will send UPDATE messages
    for (int i = 0; i < N_CLIENTS; i++) {
      system.actorOf(Client.props(replicas), "client" + i);
    }

    System.out.println("Current java version is " + System.getProperty("java.version"));
    System.out.println(">>> Press ENTER to exit <<<");
    try {
      System.in.read();
    } catch (IOException ioe) {
    } finally {
      system.terminate();
    }
  }
}
