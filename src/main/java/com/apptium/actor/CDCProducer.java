package com.apptium.actor;

import java.util.HashMap;
import java.util.List;

import org.thymeleaf.util.StringUtils;

import com.apptium.EventstoreApplication;
import com.apptium.util.CommonMethods;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class CDCProducer extends AbstractActor{

	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	@Override
	public Receive createReceive() {
		
		return this.receiveBuilder()
				.match(String.class, x-> {
					
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
						
						if(transactionMessage.getAsJsonObject().has("accountName")) 
							transactionMessage.getAsJsonObject().addProperty("accountName", accountName);
						if(transactionMessage.getAsJsonObject().has("appname")) 
							transactionMessage.getAsJsonObject().addProperty("appname", applicationName);
						if(transactionMessage.getAsJsonObject().has("eventId")) 
							transactionMessage.getAsJsonObject().addProperty("eventId", eventId);
						transactionMessage.getAsJsonObject().addProperty("eventName", eventName);
						transactionMessage.getAsJsonObject().addProperty("domainId", domainId);
						transactionMessage.getAsJsonObject().addProperty("domainName", domainName);
						transactionMessage.getAsJsonObject().addProperty("subDomainId", subDomainId); 
						transactionMessage.getAsJsonObject().addProperty("subDomainName", sudDomainName);
						transactionMessage.getAsJsonObject().addProperty("transactionDate", transactionDate);
						
						String objectkey = String.format("%s::%s::Object", accountName,applicationName); 
						String objectJson = EventstoreApplication.getCacheBPMN().get(objectkey); 
						
						DocumentContext jsonContext = JsonPath.parse(objectJson); 
						List<HashMap<String,Object>> j  = jsonContext.read(String.format("$..objects[?(@.model == '%s')]", domainName)); 
						Boolean postToCDC = true; 
						if(j.isEmpty()) {
							log.error(String.format("Could not find OBJECT Model at %s for Domain/Model %s", objectkey,domainName));
							String objectModel = StringUtils.substring(domainName, 0, domainName.length()-1); 
							 j  = jsonContext.read(String.format("$..objects[?(@.model == '%s')]", objectModel));
							 if(j.isEmpty()) {
								 log.error(String.format("Second Attempt: Could not find OBJECT Model at %s for Domain/Model %s", objectkey,objectModel));
								 postToCDC = false; 
							 }
							 transactionMessage.getAsJsonObject().addProperty("objectModel", objectModel);
						}else {
							transactionMessage.getAsJsonObject().addProperty("objectModel", domainName);
						}
						j.clear();
						j = null; 
						
						
						
						if(postToCDC) {
							log.error("Sending to CDC" +transactionMessage.toString());
							CommonMethods.sendToCDCQueue(transactionMessage.toString());
						}else {
							log.error("Sending to CDC CDCQueueFailOut" +transactionMessage.toString());
							CommonMethods.sendToCDCQueueFailOut(transactionMessage.toString());
						}
					}
				}).build(); 
	}
	
	
	
	
	/**
	 * Translate the event id to event name
	 * @param eventId
	 * @return event name
	 * @throws Exception 
	 */
	private String getEvent(String eventId) throws Exception {
//		HashMap<String,String> eventData = new HashMap<String,String>(); 
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
	    	 log.error(ex.getMessage());
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
	    	 log.error(ex.getMessage());
	     }
	     
	     return domainId; 

	}

 private String getDomain(String eventId) throws Exception {
		
//		if(EventstoreApplication.domains.asMap().containsKey(eventId)) {
//			String item = EventstoreApplication.domains.asMap().get(eventId); 
//			if(item != null)
//				return EventstoreApplication.domains.asMap().get(eventId); 
//		}
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
