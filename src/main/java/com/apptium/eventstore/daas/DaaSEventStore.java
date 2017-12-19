package com.apptium.eventstore.daas;

import java.sql.Timestamp;
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

@Component
public class DaaSEventStore {
	static final Logger LOG = LoggerFactory.getLogger(DaaSEventStore.class);
	
	final String TRANSACTIONLOG = "eventStores"; 



		
		@SuppressWarnings("unchecked")
		public void save(Map<String,Object> pLogMsg) {
			
			String URL = String.format("%s%s",EventstoreApplication.prop.getProperty("DAASURL"),TRANSACTIONLOG); 
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create() ;
		
			pLogMsg.put("id", UUID.randomUUID().toString()); 
			Timestamp createdDate = CommonMethods.getCurrentDate();
			pLogMsg.put("createdDate", System.currentTimeMillis()); 
			
			String tlog = gson.toJson(pLogMsg,pLogMsg.getClass());
			if(CommonMethods.invokeExecution(URL,tlog,new RestTemplate())){
				LOG.debug(String.format("Event Store %s - %s successfully created", 
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
			}else{
				LOG.error(String.format("Event Store %s - %s successfully not created",
						pLogMsg.get("objectId").toString(), pLogMsg.get("eventId").toString()));
			}
				
			
		}
}
