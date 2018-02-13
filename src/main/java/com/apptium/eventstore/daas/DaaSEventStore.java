package com.apptium.eventstore.daas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.apptium.eventstore.EventstoreApplication;
import com.apptium.eventstore.models.EventStoreEntry;
import com.apptium.eventstore.util.CommonMethods;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Component
public class DaaSEventStore {
	static final Logger LOG = LoggerFactory.getLogger(DaaSEventStore.class);
	
	final String TRANSACTIONLOG = "eventStores"; 
	final String BETWEENSEARCH = "eventStores/search/findByTimeStampBetween?start=1513712923340&end=1513717944317";



		
		public void save(Map<String,Object> pLogMsg) {
			
			String URL = String.format("%s%s",EventstoreApplication.prop.getProperty("DAASURL"),TRANSACTIONLOG); 
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create() ;
		
			pLogMsg.put("id", UUID.randomUUID().toString()); 
			//Timestamp createdDate = CommonMethods.getCurrentDate();
			pLogMsg.put("createdDate", System.currentTimeMillis()); 
			
			String tlog = gson.toJson(pLogMsg,pLogMsg.getClass());
			if(CommonMethods.invokeExecution(URL,tlog,new RestTemplate())){
				LOG.debug(String.format("Event Store %s - %s successfully created", 
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
				writePushNotification(pLogMsg); 
			}else{
				LOG.error(String.format("Event Store %s - %s not created",
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
			}
				
			
		}
		
		/**
		 * POST the event message to the Notification for Push Processing
		 * @param pLogMsg
		 */
		private void writePushNotification(Map<String,Object> pLogMsg) {
			
			String URL = EventstoreApplication.prop.getProperty("PUSHNOTIFICATIONURL"); 
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create() ;
			String tlog = gson.toJson(pLogMsg,pLogMsg.getClass());
			
			if(CommonMethods.invokeExecution(URL,tlog,new RestTemplate())){
				LOG.debug(String.format("Event Store Push Notification for %s - %s successfully created", 
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
			}else{
				LOG.error(String.format("Event Store Push Notification %s - %s not created",
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
			}
		}
		
		public List<EventStoreEntry> retreiveFromEventStore(Long start, Long end) {
			
			String URL = String.format("%s%s?start=%s&end=%s",EventstoreApplication.prop.getProperty("DAASURL"),BETWEENSEARCH,start,end); 
			
			Object result = CommonMethods.invokeGetExecution(URL,"{}", new RestTemplate()); 
			List<EventStoreEntry> events = new ArrayList<EventStoreEntry>(); 
			if(result == null) return events;  
			
		    JsonParser parser = new JsonParser(); 
			JsonObject t =  (JsonObject) parser.parse(result.toString()); 
			JsonArray constraintsArray = null;
			
			
			JsonObject r = t.get("_embedded").getAsJsonObject(); 
			if(r.has("eventStores")){
				constraintsArray = r.get("eventStores") != null ? r.get("eventStores").getAsJsonArray() : new JsonArray();
				
				for (int i = 0;i < constraintsArray.size();i++) {
					try {
						JsonElement q = constraintsArray.get(i);
						JsonObject item =  q.getAsJsonObject(); 
						if(item.has("id")) {
							EventStoreEntry entry = new EventStoreEntry(); 
							entry.setId(item.get("id").getAsString());
							entry.setObjectId(item.get("objectId").getAsString());
							entry.setEventId(item.get("eventId").getAsString());
							entry.setUrl(item.get("url").getAsString());
							entry.setTimeStamp(item.get("timeStamp").getAsLong());
							events.add(entry); 
						}
						
					} catch (Exception e) {
							LOG.error(e.getMessage());
					}
				}	
			}
			
			return events; 
		}
}
