package com.apptium.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;

import com.apptium.daas.DaaSEventStore;
import com.apptium.util.ServiceFault;
import com.apptium.util.ServiceFaultException;

import io.spring.guides.gs_producing_web_service.EventRequest;
import io.spring.guides.gs_producing_web_service.EventResponse;

@Endpoint
public class EventStoreSoapService {

	Logger LOG = LoggerFactory.getLogger(EventStoreSoapService.class);
	private static final String NAMESPACE_URI = "http://spring.io/guides/gs-producing-web-service";

	

	@PayloadRoot(namespace = NAMESPACE_URI, localPart = "eventRequest")
	@ResponsePayload
	public EventResponse writeEvent(@RequestPayload EventRequest request) {
		EventResponse response = new EventResponse();
		
		 Map<String,Object> map = new HashMap<String,Object>(); //gson.fromJson(eventJson, type); 
		 
		 if(request.getEventId() != null) {
			 map.put("eventId", request.getEventId()); 
		 }else {
			
			 throw new ServiceFaultException("ERROR",new ServiceFault(
		                "NOT_FOUND", "Event Store Message is missing the eventId"));
		 }
		 
		 if(request.getObjectData() != null) {
			 map.put("eventdata", request.getObjectData()); 
		 }
		 
		 if(request.getObjectId() != null) {
			 map.put("objectId", request.getObjectId()); 
		 }else {
			 throw new ServiceFaultException("ERROR",new ServiceFault(
		                "NOT_FOUND", "Event Store Message is missing the objectId"));
		 }
		 
		 Long  longValue = request.getTimeStamp(); 
		 if(longValue != null) {
			 map.put("timeStamp", request.getTimeStamp()); 
		 }
		 
		 if(longValue == null)
			 throw new ServiceFaultException("ERROR",new ServiceFault(
		                "NOT_FOUND", "Event Store Message is missing the timeStamp"));
		 
			
		 if(request.getUrl() != null) {
			 map.put("url", request.getUrl()); 
		 }else {
			 throw new ServiceFaultException("ERROR",new ServiceFault(
		                "NOT_FOUND", "Event Store Message is missing the url"));
		 }
		 
		 if(request.getAccountname() != null) {
			 map.put("accountname", request.getAccountname()); 
		 }else {
			 throw new ServiceFaultException("ERROR",new ServiceFault(
		                "NOT_FOUND", "Event Store Message is missing the AccountName"));
		 }
		 
		 if(request.getAppname() != null) {
			 map.put("appname", request.getAppname()); 
		 }else {
			 throw new ServiceFaultException("ERROR",new ServiceFault(
		                "NOT_FOUND", "Event Store Message is missing the AppName"));
		 }
		 	 
		 
		CompletableFuture.supplyAsync(() -> writeEventStoreEntry(map)); 
		response.setStatus("Successfully");
		return response;
	}
	
	private Future<String> writeEventStoreEntry(Map<String,Object> eventMessage){
		CompletableFuture<String> output = new CompletableFuture<>();
		try {
			  DaaSEventStore daasObject = new DaaSEventStore(); 
			  daasObject.save(eventMessage);
			  daasObject.writePushNotification(eventMessage);
			  output.complete("success");
		}catch (Exception e) {
			  output.completeExceptionally(e);
		}
			
		return output; 
	}
}
