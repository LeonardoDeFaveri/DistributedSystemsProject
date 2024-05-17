package it.unitn.ds1;
import akka.actor.Props;
import akka.actor.AbstractActor;
import java.io.Serializable;

// The Receiver actor
public class Receiver extends AbstractActor {

  // This class represents a message our actor will receive
  public static class Hello implements Serializable {
    public final String msg;
    public Hello(String msg) {
      this.msg = msg;
    }
  }

  // Some stuff required by Akka to create actors of this type
  static public Props props() {
    return Props.create(Receiver.class, () -> new Receiver());
  }

  // Here we define our reaction on the received Hello messages
  private void onHello(Hello h) {
    System.out.println("[" +
            getSelf().path().name() +      // the name of the current actor
            "] received a message from " +
            getSender().path().name() +    // the name of the sender actor
            ": " + h.msg                   // finally the message contents
    );
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Hello.class, this::onHello).build();
  }
}
