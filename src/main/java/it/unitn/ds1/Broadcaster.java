package it.unitn.ds1;

import java.io.IOException;
import java.util.ArrayList;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.models.JoinGroupMsg;
import it.unitn.ds1.models.StartMsg;

public class Broadcaster {
  final static int N_CLIENTS = 5;
  final static int N_REPLICAS = 5;

  public static void main(String[] args) {
    final ActorSystem system = ActorSystem.create("project");

    final ArrayList<ActorRef> replicas = new ArrayList<>();
    final ArrayList<ActorRef> clients = new ArrayList<>();

    for (int i = 0; i < N_REPLICAS; i++) {
      // Initially the coordinator is the first created replica
      replicas.add(system.actorOf(Replica.props(0, 0), "replica" + i));
    }

    for (ActorRef replica : replicas) {
      replica.tell(new JoinGroupMsg(replicas), ActorRef.noSender());
    }

    // Creates all the clients that will send UPDATE messages
    for (int i = 0; i < N_CLIENTS; i++) {
      clients.add(system.actorOf(Client.props(replicas), "client" + i));
    }

    System.out.print(">>> System ready");
    requestContinue(system);

    // Send StartMsg to clients so that they beginning producing requests
    for (ActorRef client : clients) {
      client.tell(new StartMsg(), ActorRef.noSender());
    }

    System.out.print(">>> System working");
    requestContinue(system);
    system.terminate();

    System.out.println(">>> System terminated");

    // System.out.println("Current java version is " + System.getProperty("java.version"));
  }

  private static void requestContinue(ActorSystem system) {
    System.out.println(">>> Press ENTER to continue <<<");
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
