package com.apptium.eventstore.actors;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.Materializer;
import akka.stream.Supervision;
import akka.stream.Supervision.Directive;
import akka.stream.javadsl.Flow;
import scala.Function1;

public abstract class ConsumerBase {

	 protected final ActorSystem system = ActorSystem.create("notificationStream");

	 final Function1<Throwable, Directive> decider = exc -> {
		  if (exc instanceof ArithmeticException) {
			  System.out.println(" >>>>>> resuming");
			  return Supervision.resume(); 
		  }else if(exc instanceof RuntimeException) {
			  System.out.println(" >>>>>> restart");
			  return Supervision.restart();  
		  }
		  else {
			  System.out.println(" >>>>>> resuming"); 
		    return Supervision.resume(); 
		  }
		};

	  protected final Materializer materializer = ActorMaterializer.create(ActorMaterializerSettings.create(system).withSupervisionStrategy(decider),
			  system); 


	  protected final int maxPartitions = 100;

	  protected <T> Flow<T, T, NotUsed> business() {
	    return Flow.create();
	  }
}
