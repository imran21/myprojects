package com.apptium.eventstore.actors;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apptium.eventstore.EventstoreApplication;
import com.apptium.eventstore.daas.DaaSEventStore;
import com.apptium.eventstore.util.CommonMethods;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import akka.Done;

public class OffsetStorage {
	Logger LOG = LoggerFactory.getLogger(OffsetStorage.class);
	
	 // #plainSource
	public final AtomicLong offsetStore = new AtomicLong();
    private final String groupName = EventstoreApplication.PLATFORM_KAFKA_GROUP; 
    private final String topic = EventstoreApplication.PLATFORM_KAFKA_TOPIC; 
    private final int partition = EventstoreApplication.PLATFORM_KAFKA_PARTITION; 
    private final String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 
    private String topicOffSetId; 

  // #plainSource
    public CompletionStage<Done> businessLogicAndStoreOffset(ConsumerRecord<String, byte[]> record) { 
  // #plainSource
    		DaaSEventStore daasObject = new DaaSEventStore(); 	
    	
    			String s = new String(record.value());
    			LOG.info(String.format(">>>  key = %s,  offset= %d, partition = %s, value = %s ",record.key(),record.offset(),record.partition(),s));
    			offsetStore.set(record.offset());
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
//						if(retryCounter >= EventstoreApplication.RETRYLIMIT) {
//							CommonMethods.sendToEventQueueFallOut(message.toString());
//							LOG.error(String.format("<<>> Send to EventQueueFallout Exception Retry limit reached >>>   offset = %d, value = %s ",record.offset(), s));
//						
//							
//						}else {
							if(message.has("eventId")) {
								LOG.info(String.format("internal event %s", message.get("eventId").getAsString()));
								 daasObject.process2(message.toString(), accountName,appName,message.get("eventId").getAsString());
							}else {
								 daasObject.process(message.toString(), accountName,appName);
							}
						//}
						
						 try {
							 if(topicOffSetId != null) 
								 updateExternalOffSet(groupName,record.topic(),record.partition(),offsetStore.get()+1,topicOffSetId);
					      }catch(Exception e) {
					    	  e.printStackTrace();
					      }
						 
					}else {
						LOG.error(String.format("<<>> Send to EventQueueFallout Exception no accouName >>>  offset = %d, value = %s ",record.offset(), s));
						CommonMethods.sendToEventQueueFallOut(message.toString());
					}
				}catch(Exception ex) {
				
					CommonMethods.StreamExceptionHandler(s,record.offset(),ex.getLocalizedMessage()); 
				}
				  
      return CompletableFuture.completedFuture(Done.getInstance());
    }

    
    public CompletionStage<Done> retry(ConsumerRecord<String,byte[]> record){
    
    // #plainSource
      		DaaSEventStore daasObject = new DaaSEventStore(); 	
      	
      			String s = new String(record.value());
      			LOG.info(String.format(">>>  RETRY key = %s,  offset= %d, partition = %s, value = %s ",record.key(),record.offset(),record.partition(),s));
      			
      			offsetStore.set(record.offset());
  				try {

  					JsonParser jsonParser = new JsonParser();
  					JsonElement dmnTree = jsonParser.parse(s); 
  					JsonObject message = dmnTree.getAsJsonObject();
  					if(message.has("accountName")) {
  						String accountName = message.get("accountName").getAsString(); 
  						String appName = message.get("appname").getAsString();
  						Integer retryCounter = 0; 
  						Long retryDuration = (long) 60000; 
  						
  						
  						if(message.has("retry")) {
  							retryCounter = message.get("retry").getAsInt(); 
  							retryDuration = message.get("retryDuration").getAsLong();
  		  					if(!CommonMethods.isNotificationMSAvailable()) {
  		  						LOG.warn("PROCESSING RETRY Required service or services unavailable - Check DMN and Polyglot status waiting " + retryDuration); 
  		  						Thread.sleep(retryDuration);
  		  					}
  		  					LOG.info(String.format(">>> PROCESSING RETRY key = %s,  offset= %d, partition = %s, value = %s ",record.key(),record.offset(),record.partition(),s));
  						}
//  						if(retryCounter >= EventstoreApplication.RETRYLIMIT) {
//  							CommonMethods.sendToEventQueueFallOut(message.toString());
//  							LOG.error(String.format("<<>> Send to EventQueueFallout Exception Retry limit reached >>>   offset = %d, value = %s ",record.offset(), s));
//  							
//  						}else {
  							if(message.has("eventId")) {
  								LOG.info(String.format("internal event %s", message.get("eventId").getAsString()));
  								daasObject.process2(message.toString(), accountName,appName,message.get("eventId").getAsString());
  							}else {
  								daasObject.process(message.toString(), accountName,appName);
  							}
  						//}
  						
  						 try {
  							 if(topicOffSetId != null) 
  								 updateExternalOffSet(groupName,record.topic(),record.partition(),offsetStore.get()+1,topicOffSetId);
  					      }catch(Exception e) {
  					    	  e.printStackTrace();
  					      }
  						 
  					}else {
  						LOG.error(String.format("<<>> Send to EventQueueFallout Exception no accouName >>>  offset = %d, value = %s ",record.offset(), s));
  						CommonMethods.sendToEventQueueFallOut(message.toString());
  					}
  				}catch(Exception ex) {
  				
  					CommonMethods.StreamExceptionHandler(s,record.offset(),ex.getLocalizedMessage()); 
  				}
  				  
        return CompletableFuture.completedFuture(Done.getInstance());
  }
  // #plainSource
    public CompletionStage<Long> loadOffset() { // ... }
  // #plainSource
    	try {
			JsonObject rs = getExternalOffSet(groupName,topic,partition);
			if(rs == null || rs.isJsonNull()) {
				createExternalOffSet(groupName,topic,partition,offsetStore.get()); 
			}else {
				offsetStore.set(rs.get("offset").getAsLong());	
				topicOffSetId = rs.get("id").getAsString(); 
			}
		}catch(Exception ex) {
			offsetStore.set(-1);
		}
		
      LOG.debug(String.format("%d current offset partition: %d topic : %s groupName : %s", offsetStore.get(), partition,topic,groupName));
      return CompletableFuture.completedFuture(offsetStore.get());
    }
    
    public CompletionStage<Long> loadOffset(String retryTopic) { // ... }
  // #plainSource
    	try {
			JsonObject rs = getExternalOffSet(groupName,retryTopic,partition);
			if(rs == null || rs.isJsonNull()) {
				createExternalOffSet(groupName,retryTopic,partition,offsetStore.get()); 
			}else {
				offsetStore.set(rs.get("offset").getAsLong());	
				topicOffSetId = rs.get("id").getAsString(); 
			}
		}catch(Exception ex) {
			offsetStore.set(-1);
		}
		
      LOG.debug(String.format("%d current offset partition: %d topic : %s groupName : %s", offsetStore.get(), partition,retryTopic,groupName));
      return CompletableFuture.completedFuture(offsetStore.get());
    }
    
    public CompletionStage<Map<TopicPartition, Object>> getOffSetOnAssign(Set<TopicPartition> t){
    	
    	TopicPartition  tp = null; 
    	HashMap<TopicPartition, Object> r = new HashMap<TopicPartition,Object>(); 
    	if( t.iterator().hasNext()) {
    		tp = t.iterator().next();
    		try {
    			JsonObject rs = getExternalOffSet(groupName,tp.topic(),tp.partition());
    			if(rs == null || rs.isJsonNull()) {
    				createExternalOffSet(groupName,tp.topic(),tp.partition(),offsetStore.get()); 
    			}else {
    				offsetStore.set(rs.get("offset").getAsLong());	
    				topicOffSetId = rs.get("id").getAsString(); 
    			}
    			r.put(tp, offsetStore.get()); 
    		}catch(Exception ex) {
    			r.put(tp, (long)-1); 
    		}
    		
    	}else {
    		r.put(tp, (long)-1); 
    	}
    	if(tp != null)  LOG.info("calling getOffSetOnAssign >> "+ tp.partition() + " >>> "+ tp.topic()); 
    	return CompletableFuture.completedFuture(r);
    }

    /**
     * 
     * @param groupName
     * @param topicName
     * @param partitionid
     * @return
     * @throws Exception 
     */
	private JsonObject getExternalOffSet(String groupName, String topicName,Integer partitionid) throws Exception {
		Object distResult = null; 
		JsonObject result = null; 
		String URL = String.format("%s/topicOffSets/search/findAllByGroupnameAndTopicnameAndPartitionid?groupname=%s&topicname=%s&partitionid=%d",
							DAASURL, 
							groupName,
							topicName,
							partitionid);
		
		try {
			distResult = CommonMethods.invokeGetExecution(URL, "{}", null);
			if(distResult != null) {
				JsonParser jsonParser = new JsonParser();
				JsonElement dmnTree = jsonParser.parse(distResult.toString()); 
				JsonObject tableBody = dmnTree.getAsJsonObject();
				if(tableBody.has("_embedded")) {
					JsonObject _embedded = tableBody.get("_embedded").getAsJsonObject(); 
					JsonArray offSets = _embedded.get("topicOffSets").getAsJsonArray(); 
					if(offSets.size() > 1) { 
						LOG.error(String.format("group %s topic %s partition %d has more than one entry in topicOffSets using index 0", 
							groupName, topicName, partitionid));
					}else if(offSets.size() == 1) {
						if(offSets.get(0).getAsJsonObject().has("offset"))
							result = offSets.get(0).getAsJsonObject();
					
					}
					
				}
			}
		} catch (Exception e) {
			LOG.error(e.getLocalizedMessage());
			throw new Exception(e.getLocalizedMessage());
		} 
		
		
		return result; 
	}
	
   /***
    * 
    * @param groupName
    * @param topicName
    * @param partitionId
    * @param offSet
 * @throws Exception 
    */
	private void createExternalOffSet(String groupName, String topicName,Integer partitionId ,Long offSet) throws Exception {
	
		String URL = String.format("%s/topicOffSets",DAASURL);
		JsonObject requestBody = new JsonObject(); 
		Gson gson = new Gson(); 
		requestBody.addProperty("groupname", groupName);
		requestBody.addProperty("topicname", topicName);
		requestBody.addProperty("partitionid", partitionId);
		requestBody.addProperty("offset", offSet);
		String key = String.format("%s.%s", topicName,groupName); 
		
			Object distResult = CommonMethods.invokePostExecution(URL, requestBody.toString(), null);
			if(distResult != null) {
				LOG.info(String.format("%s %s %d %d topic off set created", groupName,topicName,partitionId,offSet));
				JsonParser jsonParser = new JsonParser();
				JsonElement dmnTree = jsonParser.parse(gson.toJson(distResult)); 
				JsonObject tableBody = dmnTree.getAsJsonObject();
				topicOffSetId = tableBody.get("id").getAsString(); 
			}else {
				String msg = String.format("%s %s %d %d topic off set not create", groupName,topicName,partitionId,offSet);
				LOG.error (msg);
				throw new Exception(msg); 
		   }

	}
	/**
	 * 
	 * @param groupName
	 * @param topicName
	 * @param partitionId
	 * @param offSet
	 * @param Id
	 */
	public void updateExternalOffSet(String groupName,String topicName,Integer partitionId,Long offSet,String Id)
	{
		String URL = String.format("%s/topicOffSets/%s",DAASURL,Id);
		JsonObject requestBody = new JsonObject(); 
		requestBody.addProperty("id", Id);
		requestBody.addProperty("groupname", groupName);
		requestBody.addProperty("topicname", topicName);
		requestBody.addProperty("partitionid", partitionId);
		requestBody.addProperty("offset", offSet);
		String key = String.format("%s.%s", topicName,groupName); 
			Object distResult = CommonMethods.invokePutExecution(URL, requestBody.toString(), null);
			if(distResult != null && Boolean.parseBoolean(distResult.toString())) {
				LOG.info(String.format("%s %s %d %d %s topic off set updated", groupName,topicName,partitionId,offSet,Id));
				
				if(EventstoreApplication.getCacheBPMN().exist(key)) {
					EventstoreApplication.getCacheBPMN().remove(key, Id, 0);
					CommonMethods.reSyncRetry(1);
					CommonMethods.reSyncRetry(2);
					CommonMethods.reSyncRetry(3);
				}
			}else {
				LOG.error (String.format("%s %s %d %d %s topic off set not update", groupName,topicName,partitionId,offSet,Id));
				EventstoreApplication.getCacheBPMN().put(key, Id, requestBody.toString(), 80);
			}

	}
  
	
	
	
	
}
