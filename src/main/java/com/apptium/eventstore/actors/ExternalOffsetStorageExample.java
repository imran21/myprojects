package com.apptium.eventstore.actors;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.apptium.eventstore.EventstoreApplication;
import com.apptium.eventstore.daas.DaaSEventStore;
import com.apptium.eventstore.util.CommonMethods;
import com.google.gson.Gson;
import com.typesafe.config.Config;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Function;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;


public class ExternalOffsetStorageExample extends ConsumerBase {

final Config config = system.settings().config().getConfig("akka.kafka.consumer");
	
	private final LoggingAdapter log = Logging.getLogger(system, this);
	
	final ConsumerSettings<String, byte[]> consumerSettings =
		    ConsumerSettings.create(config, new StringDeserializer(), new ByteArrayDeserializer())
		        .withBootstrapServers(CommonMethods.getKafkaBootStrap())
		        .withGroupId(EventstoreApplication.PLATFORM_KAFKA_GROUP)
		        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
		        .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
		        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	
	
	 DaaSEventStore daasObject = new DaaSEventStore(); 	
	Gson gson = new Gson(); 
	public static void main(String[] args) {
		new ExternalOffsetStorageExample().reSyncOffSet(-1);
	    new ExternalOffsetStorageExample().demo();
	 }

	  public void demo() {
	
	    final OffsetStorage db = new OffsetStorage();

	    ActorRef rebalanceListener = this.system.actorOf(Props.create(RebalanceListenerCallbacks.class));
	  
//	    RestartSource.withBackoff(
//		        java.time.Duration.of(1, ChronoUnit.MINUTES),
//		        java.time.Duration.of(1, ChronoUnit.HOURS),
//		        0.2,
//		        () ->
//		        Source.fromCompletionStage(
//		        		db.loadOffset().thenApply(fromOffset -> {		     
//		        			return Consumer
//		        					.plainSource(
//		        							consumerSettings,
//		        							Subscriptions.assignmentWithOffset(
//		        									new TopicPartition(EventstoreApplication.PLATFORM_KAFKA_TOPIC, /* partition: */  EventstoreApplication.PLATFORM_KAFKA_PARTITION),
//		        									fromOffset
//		        							).withRebalanceListener(rebalanceListener)
//		        					 )
//		        					//.flatMapMerge(maxPartitions, Pair::second)
//		        					.mapAsync(1, db::businessLogicAndStoreOffset)
//					        	    .watchTermination(
//					        	    		(control,completionStage)->{
//					        	    			return control.shutdown();
//					        	    		}
//					        	    		
//					        	     )
//		        					.to(Sink.ignore())
//		        				    .run(materializer);
//		        })
//		     )
//	  ).runWith(Sink.ignore(), materializer); 
	    /**
	     *   new design with the restart feature  - TW 
	     */
	    
	    final Executor ec = Executors.newCachedThreadPool();
	    db.loadOffset(); 
	    try {
	    RestartSource.withBackoff(
		        java.time.Duration.of(1, ChronoUnit.MINUTES),
		        java.time.Duration.of(1, ChronoUnit.HOURS),
		        0.2,
		        () ->
		        Source.fromCompletionStage(
		        				Consumer
		        					.plainSource(
		        							consumerSettings,
		        							Subscriptions.assignmentWithOffset(
		        									new TopicPartition(EventstoreApplication.PLATFORM_KAFKA_TOPIC, /* partition: */  EventstoreApplication.PLATFORM_KAFKA_PARTITION),
		        									db.offsetStore.get()
		        							).withRebalanceListener(rebalanceListener)
		        					 )
		        				 .mapAsync(1, db::businessLogicAndStoreOffset)
		                          .watchTermination(
		                            (control, completionStage) ->
		                                completionStage.handle((res, ex) -> control.drainAndShutdown(completionStage, ec))
		                             )
		        					.to(Sink.ignore())
		        				    .run(materializer)
		     )
	  ).runWith(Sink.ignore(), materializer);   
	    
	    }catch(Exception ex) {
	    	log.error(ex.getLocalizedMessage());
	    	ex.printStackTrace();
	    }
	   
	    
	}
		        
		
	public void retry(Integer retryId) {
		
		  final OffsetStorage db = new OffsetStorage();

		    final Executor ec = Executors.newCachedThreadPool();
		    ActorRef rebalanceListener = this.system.actorOf(Props.create(RebalanceListenerCallbacks.class));

		    String topic = String.format("%sRetry%d", EventstoreApplication.PLATFORM_KAFKA_TOPIC,retryId);
		    db.loadOffset(topic); 
		    try {
		    RestartSource.withBackoff(
			        java.time.Duration.of(1, ChronoUnit.MINUTES),
			        java.time.Duration.of(1, ChronoUnit.HOURS),
			        0.2,
			        () ->
			        Source.fromCompletionStage(
		    			Consumer
		    				 .plainSource(
		    			                consumerSettings,
		    			                Subscriptions.assignmentWithOffset(
		    			                    new TopicPartition(topic, /* partition: */  EventstoreApplication.PLATFORM_KAFKA_PARTITION),
		    			                    db.offsetStore.get()
		    			                ).withRebalanceListener(rebalanceListener)
			        )
		    		  .mapAsync(1, db::retry)
		    		  .watchTermination(
	                            (control, completionStage) ->
	                                completionStage.handle((res, ex) -> control.drainAndShutdown(completionStage, ec))
	                             )
		                .to(Sink.ignore())
		                .run(materializer)
			        )
			        ).runWith(Sink.ignore(), materializer);   
		    
		    }catch(Exception ex) {
		    	log.error(ex.getLocalizedMessage());
		    	 ex.printStackTrace();
		    }
		    /**
		     * Example of the manual offset with automated partitioned selection 
		     * Issue assigns all partitions
		     */
//		    Consumer.DrainingControl<Done> controlCompletionStage2 =
//			        Consumer.plainPartitionedManualOffsetSource(consumerSettings,
//		    				Subscriptions.topics(topic)
//		    				 .withRebalanceListener(rebalanceListener), x->{
//		    					return db.getOffSetOnAssign(x); 
//		    				})
//		    		.flatMapMerge(maxPartitions, Pair::second)
//		    		  .mapAsync(1, db::retry)
//		                .toMat(Sink.ignore(), Keep.both())
//		                .mapMaterializedValue(Consumer::createDrainingControl)
//		                .run(materializer);
		
		    
	}
	 
	public static void startRetryConsumer(Integer retryId) {
		new ExternalOffsetStorageExample().reSyncOffSet(retryId);
		new ExternalOffsetStorageExample().retry(retryId);
	}
	
	
	private void reSyncOffSet(Integer retryId) {
		
		CommonMethods.reSyncRetry(retryId);
	}
}