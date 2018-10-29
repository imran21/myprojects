package com.apptium.util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apptium.EventstoreApplication;
import com.apptium.actor.AtLeastOnceWithBatchCommitExample;
import com.apptium.daas.DaaSEventStore;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import akka.Done;

public class AsyncThread implements Runnable {

	  private Logger log = LoggerFactory.getLogger(this.getClass());
	 // IntegrationMessage integrationMessage = null; 
//	   public AsyncThread(IntegrationMessage parameter) {
//		   integrationMessage = parameter; 
//	   }
	  
	  public AsyncThread() {
		  
	  }

	   public void run() {
		   //invokeFalloutOperations3(); 
		   enqueue(); 
		
			
			
	   }
	   
	   private void enqueue() {
			Boolean restartNeeded = false;
		   while(true) {
			   try {
//			   Stream<String> s =  EventstoreApplication.set.stream(); 
//			   log.debug(String.format("%d total pending", EventstoreApplication.set.size()));
//			 
//			   s.limit(100)
//			   .map(x->{
//				   CommonMethods.sendToEventQueue(x);
//				   return x; 
//			   })
//			   .forEach(x->{
//				  	if (x != null) {
//				  		EventstoreApplication.set.remove(x); 
//				  	}
//			  });
//			   log.debug(String.format("%d total remaining", EventstoreApplication.set.size()));
					Boolean supportingMSRunning = false; 
				
					log.debug("checking heart beat");
					supportingMSRunning = CommonMethods.isNotificationMSAvailable(); 
					if(EventstoreApplication.control != null) {
						
						if(!supportingMSRunning && !restartNeeded){
							restartNeeded = true; 
							log.error(">>> Detected Downstream Services(s) have become unavailable --");
							//throws an wake exception and does not shut down the stream tw - 6-22-18
						
//							 final CompletionStage<Done> done = EventstoreApplication.control.shutdown();
//							 
//							  done.whenComplete((value, exception) -> {
//							      log.debug(exception.getLocalizedMessage());
//							    }); 
							 
							 
						}else if(supportingMSRunning && restartNeeded) {
							log.info("Starting EventQueueFallOut Processing");
							if(invokeFalloutOperations3()) restartNeeded = false;  
						}
					}else if(EventstoreApplication.control == null && supportingMSRunning) {
						log.info("Starting EventQueue Processing Stream");
						AtLeastOnceWithBatchCommitExample.restart();
					}else {
						log.error(">>> Detected Downstream Servics(s) are unavailable at during startup -- ");
					}
					
					
					Thread.sleep(6000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				log.error("Transaction Consumer :"+e.getLocalizedMessage(), e);
				break; 
			}
		   }
	   }
	   
		private Boolean invokeFalloutOperations3() {
			
			String PLATFORM_KAFKA_CLUSTER = EventstoreApplication.PLATFORM_KAFKA_CLUSTER;
			//Long PLATFORM_KAFKA_WAITTIME =  System.getenv("PLATFORM_KAFKA_WAITTIME") != null ? Long.valueOf(System.getenv("PLATFORM_KAFKA_WAITTIME")) : 10000; 
			String  PLATFORM_KAFKA_GROUP = EventstoreApplication.PLATFORM_KAFKA_GROUP; 
			//String  PLATFORM_ZOOKEEPER_PORT = System.getenv("PLATFORM_ZOOKEEPER_PORT"); 
			String PLATFORM_KAFKA_PINQUEUE = String.format("%sFALLOUT", EventstoreApplication.PLATFORM_KAFKA_TOPIC);
			Integer PLATFORM_KAFKA_LOOPCOUNT = EventstoreApplication.PLATFORM_KAFKA_LOOPCOUNT;
			Integer PLATFORM_KAFKA_POLLTIME = EventstoreApplication.PLATFORM_KAFKA_POLLTIME;
			log.error("THIS NOT AN ERROR  - CONFIGURED PLATFORM_KAFKA_LOOPCOUNT :"+PLATFORM_KAFKA_LOOPCOUNT+" && PLATFORM_KAFKA_POLLTIME : "+PLATFORM_KAFKA_POLLTIME);
			
			
			Properties props = new Properties();
			
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PLATFORM_KAFKA_CLUSTER);
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
			//props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
			//props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, PLATFORM_KAFKA_GROUP);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	
			
			//Boolean supportingMSRunning = false; 
			
			//Integer fafInitialBatch = EventstoreApplication.FAF_INIT_BATCH; 

			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
		    
			List<String> topics = new ArrayList<String>(); 
			topics.add(PLATFORM_KAFKA_PINQUEUE); 
			log.error("THIS NOT AN ERROR  - ENTERING TRANSACTION LOOPa");
			 DaaSEventStore daasObject = new DaaSEventStore(); 			
			 Boolean falloutEmpty = false; 
							//while(true) {	
								
								//if(supportingMSRunning) {
								
										log.debug("THIS NOT AN ERROR  - Before Transaction SUBSCRIBE");
										KafkaConsumer<String, String> orderConsumer = new KafkaConsumer<>(props);
										orderConsumer.subscribe(topics);
										log.debug("THIS NOT AN ERROR  - After Transaction SUBSCRIBE");
										Gson gson = new Gson(); 

										try {
								
											int p=0;
								
											while(p < PLATFORM_KAFKA_LOOPCOUNT){
													p++;
													log.debug("THIS NOT AN ERROR  - Before Transaction POLLING");
													ConsumerRecords<String, String> records = orderConsumer.poll(PLATFORM_KAFKA_POLLTIME);
													log.debug("THIS NOT AN ERROR  - After Transaction POLLING");
													log.debug("THIS NOT AN ERROR  - P : "+p +" records count "+records.count());
													//int totalSize  = 50;//EventstoreApplication.transactionProcesing.asMap().size();
													//int mustBeCompletedBeforePolling = (int)(totalSize*(50.0f/100.0f)); 
							
													//log.error(String.format("Trying transaction %s percentage to process %s ",50,mustBeCompletedBeforePolling));
										
										//if(supportingMSRunning && EventstoreApplication.transactionProcesing.asMap().size() < fafInitialBatch) {
												//(AppStarter.transactionProcesing.asMap().isEmpty() || mustBeCompletedBeforePolling <= 20) ){
											
											for (ConsumerRecord<String, String> record : records) {
												log.debug(String.format(">>> partition = %s,offset = %d, key = %s, value = %s%n, topic = %s",
	   		            	 								record.partition(), record.offset(), record.key(), record.value(), record.topic()));
	   		            	 						if(record.topic().equalsIgnoreCase(PLATFORM_KAFKA_PINQUEUE)) {
	   		            	 							//JsonParser jsonParser = new JsonParser();
	   		            	 							//JsonElement dmnTree = jsonParser.parse(record.value()); 
	   		            	 							CommonMethods.sendToEventQueue(record.value());
//	   		            	 							JsonObject message = dmnTree.getAsJsonObject();
//	   		            	 							if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("process")) {
//	   		            	 								String accountName = message.get("accountName").getAsString(); 
//	   		            	 								String appName = message.get("appname").getAsString(); 
//	   		            	 								daasObject.process(message.toString(), accountName,appName);
//	   		            	 							}else if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("save")) {
//	   		            	 								
//	   		            	 								Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
//	   		            	 								Map<String,Object> map = gson.fromJson(record.value(), type); 
//	   		            	 								daasObject.save(map);
//	   		            	 							}else if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("write")) {
//	   		            	 								Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
//	   		            	 								Map<String,Object> map = gson.fromJson(record.value(), type); 
//	   		            	 								daasObject.writePushNotification(map);
//	   		            	 							}
	   		            	 						  
	   		            	 						}
	   		           	 						
						        				}
											 
											if(records != null ) log.error(String.format("pending records found %s", records.count()));
											orderConsumer.commitSync();
//										}else {
//											if(records.count()>0){
//												log.error("setting p value to "+PLATFORM_KAFKA_LOOPCOUNT+" as the previous still inprogress");
//												p = PLATFORM_KAFKA_LOOPCOUNT;
//											}
//											log.error(String.format("current records still processing are %s %s ",50,mustBeCompletedBeforePolling));
//										}
										if(!CommonMethods.isNotificationMSAvailable()) break; 
										if(records.count() == 0) falloutEmpty = true;
								}
								
							} catch(Exception ex) {
								log.error(ex.getMessage(), ex);
								//break; 
								
							}finally {
								orderConsumer.close();
								orderConsumer = null; 
								gson = null; 
							}
//							try {
//								Thread.sleep(6000);
//							} catch (InterruptedException e) {
//								log.error("Transaction Consumer :"+e.getMessage());
//							}
						//}
								
								
//						log.debug("checking heart beat");
//						supportingMSRunning = CommonMethods.isNotificationMSAvailable(); 
//						if(!supportingMSRunning)
//							try {
//									Thread.sleep(6000);
//							} catch (InterruptedException e) {
//								log.error("Transaction Consumer :"+e.getMessage());
//							}
						
							
							
				 //}
				return falloutEmpty; 
						
		}
		
		

}
