package it.unitn.ds1;
import java.io.IOException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class HelloExample {
  final static int N_SENDERS = 5;

  public static void main(String[] args) {
    final ActorSystem system = ActorSystem.create("project");

    // Create a single Receiver actor
    final ActorRef receiver = system.actorOf(
        Receiver.props(),    // actor class
        "receiver"     // the new actor name (unique within the system)
        );

    // Create multiple Sender actors that will send messages to the receiver
    for (int i=0; i<N_SENDERS; i++) {
      system.actorOf(
          Sender.props(receiver), // specifying the receiver actor here
          "sender" + i);    // the new actor name (unique within the system)
    }

    System.out.println("Current java version is " + System.getProperty("java.version"));
    System.out.println(">>> Press ENTER to exit <<<");
    try {
      System.in.read();
    }
    catch (IOException ioe) {}
    finally {
      system.terminate();
    }
  }
}
