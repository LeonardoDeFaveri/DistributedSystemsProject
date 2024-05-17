package it.unitn.ds1;
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Cancellable;
import it.unitn.ds1.Receiver.Hello;

// The Sender actor
public class Sender extends AbstractActor {
  private ActorRef receiver;

  public Sender(ActorRef receiver) {
    this.receiver = receiver; // this actor will be the destination of our messages
  }
  static public Props props(ActorRef receiverActor) {
    return Props.create(Sender.class, () -> new Sender(receiverActor));
  }

  @Override
  public void preStart() {

    // Create a timer that will periodically send a message to the receiver actor
    Cancellable timer = getContext().system().scheduler().scheduleWithFixedDelay(
        Duration.create(1, TimeUnit.SECONDS),               // when to start generating messages
        Duration.create(1, TimeUnit.SECONDS),               // how frequently generate them
        receiver,                                           // destination actor reference
        new Hello("Hello from " + getSelf().path().name()), // the message to send
        getContext().system().dispatcher(),                 // system dispatcher
        getSelf()                                           // source of the message (myself)
        );


  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().build(); // this actor does not handle any incoming messages
  }

}

