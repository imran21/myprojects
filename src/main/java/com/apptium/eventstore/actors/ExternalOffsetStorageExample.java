package com.apptium.eventstore.actors;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.apptium.eventstore.EventstoreApplication;
import com.apptium.eventstore.daas.DaaSEventStore;
import com.apptium.eventstore.util.CommonMethods;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;

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
	    // #plainSource
	    final OffsetStorage db = new OffsetStorage();

	    final Executor ec = Executors.newCachedThreadPool();
	    ActorRef rebalanceListener = this.system.actorOf(Props.create(RebalanceListenerCallbacks.class));
	    CompletionStage<Consumer.Control> controlCompletionStage2 =
		        db.loadOffset().thenApply(fromOffset -> {
		        	        			
	    		return Consumer
	    				 .plainSource(
	    			                consumerSettings,
	    			                Subscriptions.assignmentWithOffset(
	    			                    new TopicPartition(EventstoreApplication.PLATFORM_KAFKA_TOPIC, /* partition: */  EventstoreApplication.PLATFORM_KAFKA_PARTITION),
	    			                    fromOffset
	    			                ).withRebalanceListener(rebalanceListener)
		        )
	    		//.flatMapMerge(maxPartitions, Pair::second)
	    		  .mapAsync(1, db::businessLogicAndStoreOffset)
	                .to(Sink.ignore())
	                .run(materializer);
		        });
//	  
//	    Consumer.DrainingControl<Done> controlCompletionStage2 =
//		        Consumer.plainPartitionedManualOffsetSource(consumerSettings,
//	    				Subscriptions.topics(EventstoreApplication.PLATFORM_KAFKA_TOPIC )
//	    				 .withRebalanceListener(rebalanceListener), x->{
//	    					return db.getOffSetOnAssign(x); 
//	    				})
//	    		.flatMapMerge(maxPartitions, Pair::second)
//	    		  .mapAsync(1, db::businessLogicAndStoreOffset)
//	                .toMat(Sink.ignore(), Keep.both())
//	                .mapMaterializedValue(Consumer::createDrainingControl)
//	                .run(materializer);
//	    
	    
	 
	    
	    
	    
	    
	   // controlCompletionStage2.drainAndShutdown(ec);
//	    CompletionStage<Map<MetricName, Metric>> metrics = controlCompletionStage2.getMetrics();
//	    metrics.thenAccept(map -> {
//	    	System.out.println("Metrics: " + map); 
//	    	log.debug(map.toString());
//	    });
	    
	    
	}
		        
		
	public void retry(Integer retryId) {
		
		  final OffsetStorage db = new OffsetStorage();

		    final Executor ec = Executors.newCachedThreadPool();
		    ActorRef rebalanceListener = this.system.actorOf(Props.create(RebalanceListenerCallbacks.class));

		    String topic = String.format("%sRetry%d", EventstoreApplication.PLATFORM_KAFKA_TOPIC,retryId);
		    
		    CompletionStage<Consumer.Control> controlCompletionStage2 =
			        db.loadOffset(topic).thenApply(fromOffset -> {
			        	        			
		    		return Consumer
		    				 .plainSource(
		    			                consumerSettings,
		    			                Subscriptions.assignmentWithOffset(
		    			                    new TopicPartition(topic, /* partition: */  EventstoreApplication.PLATFORM_KAFKA_PARTITION),
		    			                    fromOffset
		    			                ).withRebalanceListener(rebalanceListener)
			        )
		    		//.flatMapMerge(maxPartitions, Pair::second)
		    		  .mapAsync(1, db::retry)
		                .to(Sink.ignore())
		                .run(materializer);
			        });
		    
		 
		    
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
//		final OffsetStorage db = new OffsetStorage();
//		String key; 
//		if(retryId == -1 ) {
//			key = String.format("%s.%s", EventstoreApplication.PLATFORM_KAFKA_TOPIC,EventstoreApplication.PLATFORM_KAFKA_GROUP);
//		}else {
//			key = String.format("%sRetry%d.%s", EventstoreApplication.PLATFORM_KAFKA_TOPIC,retryId,EventstoreApplication.PLATFORM_KAFKA_GROUP);
//		}
//		if(EventstoreApplication.getCacheBPMN().exist(key)) {
//			Map<String, String> offSetMsg = EventstoreApplication.getCacheBPMN().getAll(key, 80); 
//			offSetMsg.forEach((x,y)->{
//				String Id = x; 
//				JsonParser jsonParser = new JsonParser();
//				JsonElement dmnTree = jsonParser.parse(y); 
//				JsonObject tb = dmnTree.getAsJsonObject();
//				db.updateExternalOffSet(tb.get("groupname").getAsString(),
//						tb.get("topicname").getAsString(), 
//						tb.get("partitionid").getAsInt(),
//						tb.get("offset").getAsLong(), 
//						Id);
//				
//				
//			});
//		}
	}
}