package com.apptium.daas;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.thymeleaf.util.StringUtils;

import com.apptium.EventstoreApplication;
import com.apptium.actor.CDCProducer;
import com.apptium.model.EventStoreEntry;
import com.apptium.util.CommonMethods;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import akka.actor.ActorRef;
import akka.actor.Props;

@Component
public class DaaSEventStore {
	static final Logger LOG = LoggerFactory.getLogger(DaaSEventStore.class);
	
	final String TRANSACTIONLOG = "eventStores"; 
	final String BETWEENSEARCH = "eventStores/search/findByTimeStampBetween?start=1513712923340&end=1513717944317";
	
/**
 * 
 * @param inputMessage
 * @param accountName
 * 
 */
		@SuppressWarnings("unchecked")
		public Boolean process(String inputMessage,String accountName,String appName){
			String DMNURL = EventstoreApplication.prop.getProperty("DMNURL"); 
			String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 
			String dmnKey = String.format("%s_DMN", accountName.toUpperCase()); 
			String DMNSymptomsURL = String.format("%sname/%s/symptoms", DMNURL,dmnKey); 
			Timestamp currentUTC = CommonMethods.getCurrentDate();
			
	try {		
			if(!CommonMethods.isNotificationMSAvailable()) throw new RuntimeException("Required service or services unavailable - Check DMN and Polyglot status"); 
			
			
			Object dmnTable = CommonMethods.invokePostExecution2(DMNSymptomsURL, inputMessage, null); 
			if(dmnTable != null) {
				
				JsonParser jsonParser = new JsonParser();
				JsonElement dmnTree = jsonParser.parse(dmnTable.toString()); 
				JsonObject tableBody = dmnTree.getAsJsonObject();
				if(tableBody.has("tableBody")) {
					Gson gson = new Gson(); 
					HashMap<String,Object> map = new HashMap<String,Object>();
					//Map<String,Object> pPushMsg = null; 
				
					Set<Entry<String, JsonElement>> test  = tableBody.getAsJsonObject().entrySet(); 
						
						JsonParser jsonParser2 = new JsonParser();
						JsonElement rows = jsonParser2.parse(test.iterator().next().getValue().toString());
						JsonObject ruleItems= rows.getAsJsonObject(); 
						List<String> eventProcessed = new ArrayList<String>(); 
						for(Entry<String, JsonElement> myRow : ruleItems.entrySet()) {
							//LOG.info(myRow.getKey()+" "+myRow.getValue().toString());
							String row = myRow.getValue().toString(); 
							map = gson.fromJson(row, map.getClass()); 
							Object distResult = null; 
//							if(EventstoreApplication.dissimeninationRecords.asMap().containsKey(myRow.getKey())) {
//								//LOG.debug(String.format(" >>>> from cache %s size %d", myRow.getKey(),EventstoreApplication.dissimeninationRecords.asMap().size()));
//								distResult = EventstoreApplication.dissimeninationRecords.asMap().get(myRow.getKey()); 
//							}
//							
//							if(distResult == null) 
//							{
								 String DMNDISURL = String.format("%s/dMNDisseminations/search/findAllByRuleId?ruleId=%s", DAASURL,myRow.getKey()); 
								 distResult = CommonMethods.invokeGetExecution(DMNDISURL, "{}", null); 
							//}
							
							if(distResult != null) {
								EventstoreApplication.dissimeninationRecords.asMap().put(myRow.getKey(), distResult.toString()); 
								JsonParser djsonParser = new JsonParser();
								JsonElement jsonTree = djsonParser.parse(distResult.toString()); 
								JsonObject dResults = jsonTree.getAsJsonObject(); 
								if(dResults.has("_embedded")) {
									JsonObject _embedded = dResults.get("_embedded").getAsJsonObject(); 
									if(_embedded.has("dMNDisseminations")) {
										JsonArray eventStores = _embedded.get("dMNDisseminations").getAsJsonArray(); 
										Iterator<JsonElement> rw = eventStores.iterator(); 
										while(rw.hasNext()) {
											JsonElement x = rw.next(); 
											JsonObject item = x.getAsJsonObject(); 
											if(item.has("ruleType") && 
												item.get("ruleType").getAsString().toLowerCase().equals("event")) {
												//String ruleId = item.get("ruleId").getAsString(); 
												String eventId = item.get("eventid").getAsString(); 
												Map<String,Object> pLogMsg = new HashMap<String,Object>(); 
												
												pLogMsg.put("eventId", eventId); 
												pLogMsg.put("timeStamp", currentUTC.getTime()); 
												pLogMsg.put("eventdata", inputMessage); 
												pLogMsg.put("accountname",accountName); 
												pLogMsg.put("appname", appName); 
												pLogMsg.put("objectId",UUID.randomUUID().toString()); 
												
												  if(!eventProcessed.contains(eventId)) {
													 save(pLogMsg); 
//													 if(pPushMsg == null) pPushMsg = pLogMsg; 
//												
//													 if(pPushMsg != null && !pPushMsg.isEmpty())
														writePushNotification(pLogMsg); 
														eventProcessed.add(eventId); 
												   }
												}
											
				
											}
										}
										
									}
								}
							}
						
					}
			
			}else {
				LOG.debug("--- DISREGARDING NO Event Rules "+ inputMessage);
			}
			
			
		 }catch(RuntimeException e) {
				JsonParser jsonParser = new JsonParser();
				JsonElement message = jsonParser.parse(inputMessage); 
				message.getAsJsonObject().addProperty("accountName", accountName);
				message.getAsJsonObject().addProperty("appName", appName);
				message.getAsJsonObject().addProperty("action", "process"); 
				Integer counter = 1; 
				Long retryDuration = EventstoreApplication.RETRY_BASE_DURATION; 
				if(message.getAsJsonObject().has("retry")) {
					counter = message.getAsJsonObject().get("retry").getAsInt(); 
					retryDuration = message.getAsJsonObject().get("retryDuration").getAsLong();
					counter++;
					retryDuration = retryDuration *2; 
				}			
				message.getAsJsonObject().addProperty("retry", counter);
				message.getAsJsonObject().addProperty("retryDuration", retryDuration);
				String eventMessage = message.getAsJsonObject().toString(); 
				if(counter > EventstoreApplication.RETRYLIMIT) {
					CommonMethods.sendToEventQueueFallOut(message.toString());
					LOG.error(String.format("<<>> Send to EventQueueFallout Exception Retry limit reached >>>   retry = %d, value = %s ",counter, message.toString()));
					
				}else 
				   CommonMethods.sendToEventQueueRetry(eventMessage,counter);
		 }catch(Exception ex) {
				JsonParser jsonParser = new JsonParser();
				JsonElement message = jsonParser.parse(inputMessage); 
				message.getAsJsonObject().addProperty("accountName", accountName);
				message.getAsJsonObject().addProperty("appName", appName);
				message.getAsJsonObject().addProperty("action", "process"); 
				message.getAsJsonObject().addProperty("exception", ex.getLocalizedMessage()); 
				Integer counter = 1; 
				Long retryDuration = EventstoreApplication.RETRY_BASE_DURATION; 
				if(message.getAsJsonObject().has("retry")) {
					counter = message.getAsJsonObject().get("retry").getAsInt(); 
					retryDuration = message.getAsJsonObject().get("retryDuration").getAsLong();
					counter++;
					retryDuration = retryDuration *2; 
				}			
				message.getAsJsonObject().addProperty("retry", counter);
				message.getAsJsonObject().addProperty("retryDuration", retryDuration);
				String eventMessage = message.getAsJsonObject().toString(); 
				if(counter > EventstoreApplication.RETRYLIMIT) {
					CommonMethods.sendToEventQueueFallOut(message.toString());
					LOG.error(String.format("<<>> Send to EventQueueFallout Exception Retry limit reached >>>   retry = %d, value = %s ",counter, message.toString()));
					
				}else 
				   CommonMethods.sendToEventQueueRetry(eventMessage,counter);
		 }
	 	  return true; 
		}

		/**
		 * process message that already have their events id, internal event message from DM
		 * @param inputMessage
		 * @param accountName
		 * @param appName
		 * @param eventId
		 */
		public boolean process2(String inputMessage,String accountName,String appName,String eventId){

			Timestamp currentUTC = CommonMethods.getCurrentDate();
			
			
	try {		
			if(!CommonMethods.isNotificationMSAvailable()) throw new RuntimeException("Required service or services unavailable - Check DMN and Polyglot status"); 
			
			
	
					Map<String,Object> pLogMsg = new HashMap<String,Object>(); 
												
					pLogMsg.put("eventId", eventId); 
					pLogMsg.put("timeStamp", currentUTC.getTime()); 
					pLogMsg.put("eventdata", inputMessage); 
					pLogMsg.put("accountname",accountName); 
					pLogMsg.put("appname", appName); 
					JsonParser jsonParser = new JsonParser();
					JsonElement message = jsonParser.parse(inputMessage); 
					if(message.getAsJsonObject().has("objectId")) {
						LOG.info(String.format("internal event %s", message.getAsJsonObject().get("objectId").getAsString()));
						pLogMsg.put("objectId",message.getAsJsonObject().get("objectId").getAsString());
					}else {
						LOG.info(String.format("External event %s", inputMessage));
						pLogMsg.put("objectId",UUID.randomUUID().toString());
					}
					save(pLogMsg); 
					writePushNotification(pLogMsg); 
					
				
			
		 }catch(RuntimeException e) {
				JsonParser jsonParser = new JsonParser();
				JsonElement message = jsonParser.parse(inputMessage); 
				message.getAsJsonObject().addProperty("accountName", accountName);
				message.getAsJsonObject().addProperty("appName", appName);
				message.getAsJsonObject().addProperty("action", "process"); 
				message.getAsJsonObject().addProperty("exception", e.getLocalizedMessage()); 
				Integer counter = 1; 
				Long retryDuration = EventstoreApplication.RETRY_BASE_DURATION; 
				if(message.getAsJsonObject().has("retry")) {
					counter = message.getAsJsonObject().get("retry").getAsInt(); 
					retryDuration = message.getAsJsonObject().get("retryDuration").getAsLong();
					counter++;
					retryDuration = retryDuration *2; 
				}			
				message.getAsJsonObject().addProperty("retry", counter);
				message.getAsJsonObject().addProperty("retryDuration", retryDuration);
				String eventMessage = message.getAsJsonObject().toString(); 
				if(counter > EventstoreApplication.RETRYLIMIT) {
					CommonMethods.sendToEventQueueFallOut(message.toString());
					LOG.error(String.format("<<>> Send to EventQueueFallout Exception Retry limit reached >>>   retry = %d, value = %s ",counter, message.toString()));
					
				}else 
				   CommonMethods.sendToEventQueueRetry(eventMessage,counter);
		 }catch(Exception ex) {
				JsonParser jsonParser = new JsonParser();
				JsonElement message = jsonParser.parse(inputMessage); 
				message.getAsJsonObject().addProperty("accountName", accountName);
				message.getAsJsonObject().addProperty("appName", appName);
				message.getAsJsonObject().addProperty("action", "process"); 
				message.getAsJsonObject().addProperty("exception", ex.getLocalizedMessage()); 
				Integer counter = 1; 
				Long retryDuration = EventstoreApplication.RETRY_BASE_DURATION; 
				if(message.getAsJsonObject().has("retry")) {
					counter = message.getAsJsonObject().get("retry").getAsInt(); 
					retryDuration = message.getAsJsonObject().get("retryDuration").getAsLong();
					counter++;
					retryDuration = retryDuration *2; 
				}			
				message.getAsJsonObject().addProperty("retry", counter);
				message.getAsJsonObject().addProperty("retryDuration", retryDuration);
				String eventMessage = message.getAsJsonObject().toString(); 
				if(counter > EventstoreApplication.RETRYLIMIT) {
					CommonMethods.sendToEventQueueFallOut(message.toString());
					LOG.error(String.format("<<>> Send to EventQueueFallout Exception Retry limit reached >>>   retry = %d, value = %s ",counter, message.toString()), ex);
					
				}else 
				   CommonMethods.sendToEventQueueRetry(eventMessage,counter);
				
		 }
			return true; 
		}
		
		
		public void save(Map<String,Object> pLogMsg) throws Exception,RuntimeException {
			
			String URL = String.format("%s%s",EventstoreApplication.prop.getProperty("DAASURL"),TRANSACTIONLOG); 
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create() ;
		
			pLogMsg.put("id", UUID.randomUUID().toString()); 
			pLogMsg.put("createdDate", System.currentTimeMillis()); 
			
			String tlog = gson.toJson(pLogMsg,pLogMsg.getClass());
			try {
				if(!CommonMethods.isNotificationMSAvailable()) throw new RuntimeException("Required service or services unavailable - Check DMN and Polyglot status"); 
				if(CommonMethods.invokeExecution(URL,tlog,new RestTemplate())){
//					LOG.debug(String.format("Event Store %s - %s successfully created", 
//							pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
					//writePushNotification(pLogMsg); 
				}else{
					LOG.error(String.format("Event Store %s - %s not created",
							pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
				}
			 }catch(RuntimeException e) {
				 throw e;
			 }catch(Exception ex) {
				LOG.error(String.format("Event Store %s - %s Not Saved, Exception %s",
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString(),ex.getLocalizedMessage()), ex);
//				
//				pLogMsg.put("action", "save"); 
//				String inputMessage = gson.toJson(pLogMsg,pLogMsg.getClass());
//				CommonMethods.sendToEventQueueFallOut(inputMessage);
				throw ex; 

			}
			
		}
		
		/**
		 * POST the event message to the Notification for Push Processing
		 * @param pLogMsg
		 */
		public void writePushNotification(Map<String,Object> pLogMsg) throws RuntimeException{
			
			//String URL = EventstoreApplication.prop.getProperty("PUSHNOTIFICATIONURL"); 
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create() ;
			String tlog = gson.toJson(pLogMsg,pLogMsg.getClass());
			try {
				LOG.error("Writing to Push Queue");
				CommonMethods.sendToPushQueue(tlog);
				/**
				 * write the message to the CDC Producer Actor
				 * 
				 */
				LOG.error("Caling the CDC Producer");
				
				CompletableFuture.runAsync( new Runnable() {

					@Override
					public void run() {
						try {
							createCDCMessage(tlog); 
						}catch(Exception ex) {
							LOG.error(ex.getLocalizedMessage() +" while trying the create CDC Message");
						}
						
					}
					
				}); 
				
//				ActorRef cdcProducer = EventstoreApplication.system.actorOf(Props.create(CDCProducer.class)); 
//				cdcProducer.tell(tlog, ActorRef.noSender());
//				cdcProducer = null; 
				
//				if(CommonMethods.invokeExecution(URL,tlog,new RestTemplate())){
//					LOG.debug(String.format("Event Store Push Notification for  Object ID %s -  Event ID %s successfully created", 
//							pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
//				}else{
//					LOG.error(String.format("Event Store Push Notification Object ID %s -  Event ID %s not created",
//							pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
//				}
			}catch(Exception ex) {
				LOG.error(String.format("Event Store Push Notification Object ID %s -  Event ID %s not writtent to PushQueue, Exception %s",
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString(),ex.getLocalizedMessage()), ex);
				
				throw new RuntimeException("Required service or services unavailable - Check DMN and Polyglot status"); 
			}
		}
		
		public List<EventStoreEntry> retreiveFromEventStore(Long start, Long end) {
			
			String URL = String.format("%s%s?start=%s&end=%s",EventstoreApplication.prop.getProperty("DAASURL"),BETWEENSEARCH,start,end); 
			
			List<EventStoreEntry> events = new ArrayList<EventStoreEntry>(); 
			Object result = null; 
			try {
				result = CommonMethods.invokeGetExecution(URL,"{}", new RestTemplate());
			}catch(Exception ex) {
				LOG.error(ex.getLocalizedMessage(), ex);
				return events;
			}
			
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
							LOG.error(e.getMessage(), e);
					}
				}	
			}
			
			return events; 
		}
		
		private void createCDCMessage(String x) throws Exception {
			LOG.error(x.toString());
			JsonParser jsonParser = new JsonParser();
			JsonElement message = jsonParser.parse(x); 				
			if(message.getAsJsonObject().has("eventId")) {
				String eventId = message.getAsJsonObject().get("eventId").getAsString(); 
				String eventName = getEvent(eventId); 
				
				String subDomainId = FindSubDomainByEventId(eventId); 
				String sudDomainName = getSubDomain(subDomainId); 
				String domainId = FindDomainBySubDomainId(subDomainId); 
				String domainName = getDomain (domainId); 
				Long transactionDate = message.getAsJsonObject().get("timeStamp").getAsLong(); 
				String accountName = message.getAsJsonObject().get("accountname").getAsString(); 
				String applicationName = message.getAsJsonObject().get("appname").getAsString(); 
			
				JsonElement transactionMessage = jsonParser.parse( message.getAsJsonObject().get("eventdata").getAsString()); 
				
				if(!transactionMessage.getAsJsonObject().has("accountName")) 
					transactionMessage.getAsJsonObject().addProperty("accountName", accountName);
				if(!transactionMessage.getAsJsonObject().has("appname")) 
					transactionMessage.getAsJsonObject().addProperty("appname", applicationName);
				if(!transactionMessage.getAsJsonObject().has("eventId")) 
					transactionMessage.getAsJsonObject().addProperty("eventId", eventId);
				transactionMessage.getAsJsonObject().addProperty("eventName", eventName);
				transactionMessage.getAsJsonObject().addProperty("domainId", domainId);
				transactionMessage.getAsJsonObject().addProperty("domainName", domainName);
				transactionMessage.getAsJsonObject().addProperty("subDomainId", subDomainId); 
				transactionMessage.getAsJsonObject().addProperty("subDomainName", sudDomainName);
				transactionMessage.getAsJsonObject().addProperty("transactionDate", transactionDate);
				
				String objectkey = String.format("%s::%s::Object", accountName,applicationName); 
				String objectJson = EventstoreApplication.getCacheBPMN().get(objectkey); 
				LOG.error(objectkey); 
				DocumentContext jsonContext = JsonPath.parse(objectJson); 
				List<HashMap<String,Object>> j  = jsonContext.read(String.format("$..objects[?(@.model == '%s')]", domainName)); 
				Boolean postToCDC = true; 
				if(j.isEmpty()) {
					LOG.error(String.format("First Attempt Could not find OBJECT Model at %s for Domain/Model %s", objectkey,domainName));
					String objectModel = StringUtils.substring(domainName, 0, domainName.length()-1); 
					 j  = jsonContext.read(String.format("$..objects[?(@.model == '%s')]", objectModel));
					 if(j.isEmpty()) {
						 LOG.error(String.format("Second Attempt: Could not find OBJECT Model at %s for Domain/Model %s", objectkey,objectModel));
						 postToCDC = false; 
					 }
					 transactionMessage.getAsJsonObject().addProperty("objectModel", objectModel);
				}else {
					transactionMessage.getAsJsonObject().addProperty("objectModel", domainName);
				}
				j.clear();
				j = null; 
				
				
				
				if(postToCDC) {
					LOG.error("Sending to CDC" +transactionMessage.toString());
					CommonMethods.sendToCDCQueue(transactionMessage.toString());
				}else {
					LOG.error("Sending to CDC CDCQueueFailOut" +transactionMessage.toString());
					CommonMethods.sendToCDCQueueFailOut(transactionMessage.toString());
				}
		}
	}
		
		/**
		 * Translate the event id to event name
		 * @param eventId
		 * @return event name
		 * @throws Exception 
		 */
		private String getEvent(String eventId) throws Exception {
//			HashMap<String,String> eventData = new HashMap<String,String>(); 
		   String eventName = null; 
		   String DAASURL = EventstoreApplication.prop.getProperty("DAASURL");
			String Url = String.format("%s/events/%s",DAASURL,eventId); 
			Object results = CommonMethods.invokeGetExecution(Url, "{}", null);
			JsonParser jsonParser = new JsonParser();
		    JsonElement jsonTree = jsonParser.parse(results.toString()); 
		    JsonObject searchResults = jsonTree.getAsJsonObject(); 
		    if(searchResults.has("name")) eventName = searchResults.get("name").getAsString(); 
		    return eventName; 	
		    
		}


		private String FindSubDomainByEventId(String eventId) throws Exception {
			
			String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 

			
			 String URL = String.format("%s%s/search/findAllBySteIdEventId?eventId=%s", DAASURL,"subDomainToEvents",eventId); 
			 Object saerchResult = CommonMethods.invokeGetExecution(URL, "{}", null); 
			 String subDomainId = null;
			 if(saerchResult == null)  return null; 
			 
			 JsonParser jsonParser = new JsonParser();
		     JsonElement jsonTree = jsonParser.parse(saerchResult.toString()); 
		     JsonObject requestBody = jsonTree.getAsJsonObject(); 
		     JsonArray result = null; 
		     try {
		     if(requestBody.has("_embedded") && requestBody.get("_embedded").getAsJsonObject().has("subDomainToEvents")) 
		    	 		result = requestBody.get("_embedded").getAsJsonObject().get("subDomainToEvents").getAsJsonArray();
		     		JsonObject item = result.get(0).getAsJsonObject(); 
		     		if(item.has("steId")) { 
		     			subDomainId= item.getAsJsonObject("steId").get("subDomainId").getAsString();
		     			//EportalCdcApplication.subDomainFromEventId.asMap().put(eventId, subDomainId); 
		     		}
		     		
		     }catch(Exception ex) {
		    	 LOG.error(ex.getMessage());
		     }
		     
		     return subDomainId; 
		   

		}

		private String FindDomainBySubDomainId(String subDomainId) throws Exception {
			
		 
			 String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 
			 String URL = String.format("%s%s/search/findAllByDtsIdSubDomainId?subDomainId=%s", DAASURL,"domainToSubDomains",subDomainId); 
			 Object saerchResult = CommonMethods.invokeGetExecution(URL, "{}", null); 
			 String domainId = null;
			 if(saerchResult == null)  return null; 
			 
			 JsonParser jsonParser = new JsonParser();
		     JsonElement jsonTree = jsonParser.parse(saerchResult.toString()); 
		     JsonObject requestBody = jsonTree.getAsJsonObject(); 
		     JsonArray result = null; 
		     try {
		     if(requestBody.has("_embedded") && requestBody.get("_embedded").getAsJsonObject().has("domainToSubDomains")) 
		    	 		result = requestBody.get("_embedded").getAsJsonObject().get("domainToSubDomains").getAsJsonArray();
		     		JsonObject item = result.get(0).getAsJsonObject(); 
		     		if(item.has("dtsId")) { 
		     			domainId= item.getAsJsonObject("dtsId").get("domainId").getAsString();
		     		}
		     		
		     }catch(Exception ex) {
		    	 LOG.error(ex.getMessage());
		     }
		     
		     return domainId; 

		}

	 private String getDomain(String eventId) throws Exception {
			
//			if(EventstoreApplication.domains.asMap().containsKey(eventId)) {
//				String item = EventstoreApplication.domains.asMap().get(eventId); 
//				if(item != null)
//					return EventstoreApplication.domains.asMap().get(eventId); 
//			}
			String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 
			String Url = String.format("%s/domains/%s",DAASURL,eventId); 
			Object results = CommonMethods.invokeGetExecution(Url, "{}", null);
			JsonParser jsonParser = new JsonParser();
		    JsonElement jsonTree = jsonParser.parse(results.toString()); 
		    JsonObject searchResults = jsonTree.getAsJsonObject(); 
		    String domainName =  null;  
		    	if(searchResults.has("name")) {
		    		domainName = searchResults.get("name").getAsString();
		    		//EventstoreApplication.domains.asMap().put(eventId, domainName); 
		    	}
			return domainName; 
		}
	 
	 private String getSubDomain(String subdomainID) throws Exception {
			

			String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 
			String Url = String.format("%s/subdomains/%s",DAASURL,subdomainID); 
			Object results = CommonMethods.invokeGetExecution(Url, "{}", null);
			JsonParser jsonParser = new JsonParser();
		    JsonElement jsonTree = jsonParser.parse(results.toString()); 
		    JsonObject searchResults = jsonTree.getAsJsonObject(); 
		    String domainName =  null;  
		    	if(searchResults.has("name")) {
		    		domainName = searchResults.get("name").getAsString();
		    	}
			return domainName; 
		}		
		
}
