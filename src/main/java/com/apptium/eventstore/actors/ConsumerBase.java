package com.apptium.eventstore.actors;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;

public abstract class ConsumerBase {

	 protected final ActorSystem system = ActorSystem.create("notificationStream");

	  protected final Materializer materializer = ActorMaterializer.create(system);

	  protected final int maxPartitions = 100;

	  protected <T> Flow<T, T, NotUsed> business() {
	    return Flow.create();
	  }
}
