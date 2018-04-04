package com.apptium.eventstore.util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import com.apptium.eventstore.EventstoreApplication;
import com.apptium.eventstore.daas.DaaSEventStore;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class AsyncThread implements Runnable {

	  private Logger log = LoggerFactory.getLogger(this.getClass());
	 // IntegrationMessage integrationMessage = null; 
//	   public AsyncThread(IntegrationMessage parameter) {
//		   integrationMessage = parameter; 
//	   }
	  
	  public AsyncThread() {
		  
	  }

	   public void run() {
//		   Map<String, Object> metaData = new HashMap<String,Object>(); 
//		   MessageIntegrationSPI msgIntegrationSPI=MessageIntegrationSPI.getMessageIntegrationImpl("eportal");
//		   msgIntegrationSPI.receive(integrationMessage, metaData);
//		   log.debug("Starting Workflow");
//		  // metaData = null; 
//		   try {
//				Thread.sleep(1000);
//				metaData = null; 
//			} catch (InterruptedException e1) {
//				
//			}
		   invokeFalloutOperations3(); 
		   
	   }
	   
		private void invokeFalloutOperations3() {
			
			String PLATFORM_KAFKA_CLUSTER = EventstoreApplication.PLATFORM_KAFKA_CLUSTER;
			//Long PLATFORM_KAFKA_WAITTIME =  System.getenv("PLATFORM_KAFKA_WAITTIME") != null ? Long.valueOf(System.getenv("PLATFORM_KAFKA_WAITTIME")) : 10000; 
			String  PLATFORM_KAFKA_GROUP = EventstoreApplication.PLATFORM_KAFKA_GROUP; 
			//String  PLATFORM_ZOOKEEPER_PORT = System.getenv("PLATFORM_ZOOKEEPER_PORT"); 
			String PLATFORM_KAFKA_PINQUEUE = EventstoreApplication.PLATFORM_KAFKA_TOPIC;
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
			
			Boolean supportingMSRunning = false; 
			
			//Integer fafInitialBatch = EventstoreApplication.FAF_INIT_BATCH; 

			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
		    
			List<String> topics = new ArrayList<String>(); 
			topics.add(PLATFORM_KAFKA_PINQUEUE); 
			log.error("THIS NOT AN ERROR  - ENTERING TRANSACTION LOOPa");
			 DaaSEventStore daasObject = new DaaSEventStore(); 			
							while(true) {	
								
								if(supportingMSRunning) {
								
										log.error("THIS NOT AN ERROR  - Before Transaction SUBSCRIBE");
										KafkaConsumer<String, String> orderConsumer = new KafkaConsumer<>(props);
										orderConsumer.subscribe(topics);
										log.error("THIS NOT AN ERROR  - After Transaction SUBSCRIBE");
										Gson gson = new Gson(); 

										try {
								
											int p=0;
								
											while(p < PLATFORM_KAFKA_LOOPCOUNT){
													p++;
													log.error("THIS NOT AN ERROR  - Before Transaction POLLING");
													ConsumerRecords<String, String> records = orderConsumer.poll(PLATFORM_KAFKA_POLLTIME);
													log.error("THIS NOT AN ERROR  - After Transaction POLLING");
													log.error("THIS NOT AN ERROR  - P : "+p +" records count "+records.count());
													int totalSize  = 50;//EventstoreApplication.transactionProcesing.asMap().size();
													int mustBeCompletedBeforePolling = (int)(totalSize*(50.0f/100.0f)); 
							
													log.error(String.format("Trying transaction %s percentage to process %s ",50,mustBeCompletedBeforePolling));
										
										//if(supportingMSRunning && EventstoreApplication.transactionProcesing.asMap().size() < fafInitialBatch) {
												//(AppStarter.transactionProcesing.asMap().isEmpty() || mustBeCompletedBeforePolling <= 20) ){
											
											for (ConsumerRecord<String, String> record : records) {
												log.error(String.format(">>> partition = %s,offset = %d, key = %s, value = %s%n, topic = %s",
	   		            	 								record.partition(), record.offset(), record.key(), record.value(), record.topic()));
	   		            	 						if(record.topic().equalsIgnoreCase(EventstoreApplication.PLATFORM_KAFKA_TOPIC)) {
	   		            	 							JsonParser jsonParser = new JsonParser();
	   		            	 							JsonElement dmnTree = jsonParser.parse(record.value()); 
	   		            	 							JsonObject message = dmnTree.getAsJsonObject();
	   		            	 							if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("process")) {
	   		            	 								String accountName = message.get("accountName").getAsString(); 
	   		            	 								daasObject.process(message.toString(), accountName);
	   		            	 							}else if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("save")) {
	   		            	 								
	   		            	 								Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
	   		            	 								Map<String,Object> map = gson.fromJson(record.value(), type); 
	   		            	 								daasObject.save(map);
	   		            	 							}else if(message.has("action") && message.get("action").getAsString().equalsIgnoreCase("write")) {
	   		            	 								Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
	   		            	 								Map<String,Object> map = gson.fromJson(record.value(), type); 
	   		            	 								daasObject.writePushNotification(map);
	   		            	 							}
	   		            	 						  
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
										if(!isNotificationMSAvailable()) break; 
								}
								
							} catch(Exception ex) {
								log.error(ex.getMessage());
								break; 
								
							}finally {
								orderConsumer.close();
								orderConsumer = null; 
								gson = null; 
							}
							try {
								Thread.sleep(6000);
							} catch (InterruptedException e) {
								log.error("Transaction Consumer :"+e.getMessage());
							}
						}
								
								
						log.debug("checking heart beat");
						supportingMSRunning = isNotificationMSAvailable(); 
						if(!supportingMSRunning)
							try {
									Thread.sleep(6000);
							} catch (InterruptedException e) {
								log.error("Transaction Consumer :"+e.getMessage());
							}
						
							
							
				 }
						
		}
		
		
		
		private boolean isNotificationMSAvailable() {
			boolean available = false; 
			String DMNURL = EventstoreApplication.prop.getProperty("DMNURL"); 
			String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 
			String URL = EventstoreApplication.prop.getProperty("PUSHNOTIFICATIONURL"); 
			
			try {
				Object notification = CommonMethods.invokeGetExecution(URL,"{}", new RestTemplate());
				Object dmn = CommonMethods.invokeGetExecution(DMNURL,"{}", new RestTemplate());
				Object polyglot = CommonMethods.invokeGetExecution(DAASURL,"{}", new RestTemplate());
				
				if(notification != null && dmn != null && polyglot != null) available = true; 
			}catch(Exception ex) {
				log.error(ex.getLocalizedMessage());
			}
			
			return available; 
			
		}
}
