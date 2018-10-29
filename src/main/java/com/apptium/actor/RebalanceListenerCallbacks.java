package com.apptium.actor;

import akka.actor.AbstractLoggingActor;

public class RebalanceListenerCallbacks extends AbstractLoggingActor {

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(akka.kafka.TopicPartitionsAssigned.class, assigned -> {
            log().info("Assigned: {}", assigned);
          })
          .match(akka.kafka.TopicPartitionsRevoked.class, revoked -> {
            log().info("Revoked: {}", revoked);
          })
          .build();
    }
}
