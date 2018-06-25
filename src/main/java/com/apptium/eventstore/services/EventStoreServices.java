package com.apptium.eventstore.services;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.apptium.eventstore.EventstoreApplication;
import com.apptium.eventstore.daas.DaaSEventStore;
import com.apptium.eventstore.models.EventStoreEntry;
import com.apptium.eventstore.util.CommonMethods;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

@RestController
@RequestMapping(value="eventstore/services/v1", produces="application/json", consumes="application/json")
public class EventStoreServices {

	Logger LOG = LoggerFactory.getLogger(EventStoreServices.class);
	
	
	@RequestMapping(produces="text/plain", method=RequestMethod.GET)
	public String getIt() {
		return "Got it!";
	}
	
	@RequestMapping(method=RequestMethod.GET, path="{start}/{end}")
	public ResponseEntity<?> get(@PathVariable("start")Long start,@PathVariable("end")Long end){
		HttpHeaders headers = new HttpHeaders(); 
		try {
		 Gson gson = new Gson();
		 DaaSEventStore daasObject = new DaaSEventStore(); 
		 List<EventStoreEntry> results = daasObject.retreiveFromEventStore(start, end); 
		 return new ResponseEntity<>(gson.toJson(results,results.getClass()),
				 headers,HttpStatus.OK);
		}catch(Exception ex) {
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Error",ex.getMessage()),
						headers,HttpStatus.BAD_REQUEST);
		}
	}
	
	
	
	
	@RequestMapping(method=RequestMethod.POST)
	public ResponseEntity<?> create(@RequestBody String eventMessage) {
		HttpHeaders headers = new HttpHeaders(); 
		 Gson gson = new Gson();
		 Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
		 Map<String,Object> map = gson.fromJson(eventMessage, type); 
		 if(!map.containsKey("objectId"))
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","Event Store Message is missing objectId"),
						headers,HttpStatus.BAD_REQUEST);
		 
		 if(!map.containsKey("timeStamp"))
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","Event Store Message is missing timeStamp"),
						headers,HttpStatus.BAD_REQUEST);
		 
		 if(!map.containsKey("url"))
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","Event Store Message is missing url"),
						headers,HttpStatus.BAD_REQUEST);
		 
		 if(!map.containsKey("eventId"))
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","Event Store Message is missing eventId"),
						headers,HttpStatus.BAD_REQUEST);
		 
		 if(!map.containsKey("accountname"))
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","Event Store Message is missing AccountName"),
						headers,HttpStatus.BAD_REQUEST);
		 
		 if(!map.containsKey("appname"))
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","Event Store Message is missing AppName"),
						headers,HttpStatus.BAD_REQUEST);
		 
		 
		 
		CompletableFuture.supplyAsync(() -> writeEventStoreEntry(eventMessage)); 
	
		return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Sucessfully","Event Store Entity Created"),
				headers,HttpStatus.CREATED);
	}
	
	
	@RequestMapping(method=RequestMethod.POST,value="/{accountName}" )
	public ResponseEntity<?> createEvent(@RequestBody String eventMessage,@PathVariable("accountName")String accountName) {
		HttpHeaders headers = new HttpHeaders(); 		 
		 if(accountName == null || accountName.isEmpty())
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","AccountName is required "),
						headers,HttpStatus.BAD_REQUEST);
		 
		 if(eventMessage == null ||eventMessage.isEmpty())
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","EventMessage is required"),
						headers,HttpStatus.BAD_REQUEST);
		 
		 Gson gson = new Gson();
		 Type type = new TypeToken<Map<String,Object>>(){}.getType(); 
		 Map<String,Object> map = gson.fromJson(eventMessage, type);
		
		 
		 if(!map.containsKey("appname")) {
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Missing","Event Store Message is missing AppName"),
						headers,HttpStatus.BAD_REQUEST);
		 }
			 
		 final String appName =  map.get("appname").toString();  
		 
		CompletableFuture.supplyAsync(() -> writeEventStoreEntry(eventMessage, accountName,appName)); 
		
		return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Sucessfully","Event Store Entity Created"),
				headers,HttpStatus.CREATED);
	}
	
	
	
//	private Future<String> writeEventStoreEntry(Map<String,Object> eventMessage){
//		CompletableFuture<String> output = new CompletableFuture<>();
//		try {
//			 
//			  //DaaSEventStore daasObject = new DaaSEventStore(); 
//			  //daasObject.save(eventMessage);
//			  //daasObject.writePushNotification(eventMessage);
//			  output.complete("success");
//		}catch (Exception e) {
//			  output.completeExceptionally(e);
//		}
//			
//		return output; 
//	}
	
	private Future<String> writeEventStoreEntry(String inputMessage){
		CompletableFuture<String> output = new CompletableFuture<>();
		try {
			 
			JsonParser jsonParser = new JsonParser();
			JsonElement message = jsonParser.parse(inputMessage); 
			message.getAsJsonObject().addProperty("action", "process"); 
			String eventMessage = message.getAsJsonObject().toString(); 
			CommonMethods.sendToEventQueue(eventMessage);
			  output.complete("success");
		}catch (Exception e) {
			  output.completeExceptionally(e);
		}
			
		return output; 
	}
	
	
	
	
	
	private Future<String> writeEventStoreEntry(String inputMessage,String accountName,String appName){
		CompletableFuture<String> output = new CompletableFuture<>();
		try {
			
			 // DaaSEventStore daasObject = new DaaSEventStore(); 
			  //daasObject.process(inputMessage,accountName,appName);
			
			JsonParser jsonParser = new JsonParser();
			JsonElement message = jsonParser.parse(inputMessage); 
			message.getAsJsonObject().addProperty("accountName", accountName);
			message.getAsJsonObject().addProperty("appName", appName);
			message.getAsJsonObject().addProperty("action", "process"); 
			String eventMessage = message.getAsJsonObject().toString(); 
			CommonMethods.sendToEventQueue(eventMessage);
			  output.complete("success");
		}catch (Exception e) {
			  output.completeExceptionally(e);
		}
			
		return output; 
	}
	/**
	 * 
	 * @return
	 */
	@RequestMapping(method=RequestMethod.DELETE,value="/clear/cache/{ruleId}" )
	public ResponseEntity<?> clearCachce(@PathVariable("accountName")String ruleId) {
	
		HttpHeaders headers = new HttpHeaders(); 
		try {
			EventstoreApplication.dissimeninationRecords.asMap().remove(ruleId);
			
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Success","Cache cleared for rule "+ruleId),
						headers,HttpStatus.OK);

		}catch(UnsupportedOperationException e) {
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Error","Cache not cleared"),
						headers,HttpStatus.BAD_REQUEST);

		}
			 
		
	}
	
	@RequestMapping(method=RequestMethod.DELETE,value="/clear/cache/all" )
	public ResponseEntity<?> clearCachce() {
	
		HttpHeaders headers = new HttpHeaders(); 
		try {
			EventstoreApplication.dissimeninationRecords.asMap().clear();
			
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Success","Caches cleared"),
						headers,HttpStatus.OK);

		}catch(UnsupportedOperationException e) {
			 return new ResponseEntity<>(String.format("{\"%s\": \"%s\"}", "Error","Caches not cleared"),
						headers,HttpStatus.BAD_REQUEST);

		}
			 
		
	}


	
	
}
