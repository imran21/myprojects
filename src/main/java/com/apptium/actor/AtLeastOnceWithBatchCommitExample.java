package com.apptium.actor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.apptium.EventstoreApplication;
import com.apptium.daas.DaaSEventStore;
import com.apptium.util.CommonMethods;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Sink;


/***
 * 
 * @author taftwallaceiii
 *
 */
public class AtLeastOnceWithBatchCommitExample extends ConsumerBase {
	
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
		    new AtLeastOnceWithBatchCommitExample().demo();	   
	  }
	  
	  public static void restart() {
		  new AtLeastOnceWithBatchCommitExample().demo();	   
	  }
	
		  public void demo() {
		    // #atLeastOnceBatch
		//    Consumer.Control control =
			  EventstoreApplication.control = 
		        Consumer.committablePartitionedSource(consumerSettings, Subscriptions.topics(EventstoreApplication.PLATFORM_KAFKA_TOPIC ))
		        .flatMapMerge(maxPartitions, Pair::second)
		            .mapAsync(1, msg ->
		                business(msg.record().key(), msg.record().value(),msg.record().offset(),msg.record().partition())
		                        .thenApply(done -> msg.committableOffset())
		            )
//		            .batch(
//		                20,
//		                ConsumerMessage::createCommittableOffsetBatch,
//		                ConsumerMessage.CommittableOffsetBatch::updated
//		            )
		            .mapAsync(3, c -> c.commitJavadsl())
		            .to(Sink.ignore())
		            .run(materializer);
		    // #atLeastOnceBatch
		  }

		  CompletionStage<String> business(String key, byte[] value, long offset,int partition) { // .... }
				String s = new String(value);
				log.info(String.format(">>>  key = %s,  offset= %d, partition = %s, value = %s ",key,offset,partition,s ));
						try {
							JsonParser jsonParser = new JsonParser();
							JsonElement dmnTree = jsonParser.parse(s); 
							JsonObject message = dmnTree.getAsJsonObject();
							if(message.has("accountName")) {
								String accountName = message.get("accountName").getAsString(); 
								String appName = message.get("appname").getAsString();
								Integer retryCounter = 0; 
								
								
								if(message.has("retry")) {
									retryCounter = message.get("retry").getAsInt(); 
								}
								if(retryCounter >= EventstoreApplication.RETRYLIMIT) {
									CommonMethods.sendToEventQueueFallOut(message.toString());
									log.error(String.format("<<>> Send to EventQueueFallout Exception Retry limit reached >>>   offset = %d, value = %s ",offset, s));
									
								}else {
									if(message.has("eventId")) {
										log.info(String.format("internal event %s", message.get("eventId").getAsString()));
										daasObject.process2(message.toString(), accountName,appName,message.get("eventId").getAsString());
									}else {
										daasObject.process(message.toString(), accountName,appName);
									}
								}
//							}else if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("save")) {
//								
//								Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
//								Map<String,Object> map = gson.fromJson(s, type); 
//								daasObject.save(map);
//							}else if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("write")) {
//								Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
//								Map<String,Object> map = gson.fromJson(s, type); 
//								daasObject.writePushNotification(map);
							}else {
								log.error(String.format("<<>> Send to EventQueueFallout Exception no accouName>>>  offset = %d, value = %s ",offset, s));
								CommonMethods.sendToEventQueueFallOut(message.toString());
							}
						}catch(Exception ex) {
						
							CommonMethods.StreamExceptionHandler(s,offset,ex.getLocalizedMessage()); 
						}
						
				
		    return CompletableFuture.completedFuture("");
		  }
 }

