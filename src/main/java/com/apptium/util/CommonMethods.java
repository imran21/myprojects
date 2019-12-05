package com.apptium.util;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.apptium.EventstoreApplication;
import com.apptium.actor.OffsetStorage;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CommonMethods {
	//static final Logger logger = LogManager.getLogger(CommonMethods.class);
	
	static final Logger logger = LoggerFactory.getLogger(CommonMethods.class);
	
	public static  Boolean invokeExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate) throws Exception{
		Boolean executed = false;
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

		HttpEntity<String> entity = new HttpEntity<String>(executionRequestBody, headers);
		Object myObject;
		try{
			//ResponseEntity<String> responseString = restTemplate.exchange("http://ENGINE-SERVICE/engine/ebpmn/services/SIKOMSWrkFlow/startinstance/1", HttpMethod.POST,entity, String.class, map); 
			if(restTemplate == null){
				restTemplate = new RestTemplate(); 
			}
			ResponseEntity<String> responseString = restTemplate.exchange(executionURL, HttpMethod.POST,entity, String.class, map); 
			if(responseString != null){
				if(responseString.getStatusCode() == HttpStatus.OK){
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					//logger.debug(myObject.toString());
					
					executed = true;
				}else if(responseString.getStatusCode() == HttpStatus.CREATED){
					
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					//logger.debug(myObject.toString());
					executed = true; 
				}
			}
		}catch(RestClientException e){
			logger.error("RestClientException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody, e);
			if(e.getMessage().contains("404")){
				//throw new Exception(String.format("%s returned %s", executionURL,"HTTP 404")); 
			}else if(e.getMessage().contains("500")) {
				throw new Exception(String.format("%s returned %s", executionURL,"HTTP 500")); 
			}else {
				throw new Exception(String.format("%s returned %s", executionURL,"HTTP 500")); 
			}
				
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody, e);
			throw new Exception(String.format("%s returned %s", executionURL,"HTTP 500")); 
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody, e);
			throw new Exception(String.format("%s returned %s", executionURL,"HTTP 500")); 
		} catch (IOException e) {
			logger.error("IOException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody, e);
			throw new Exception(String.format("%s returned %s", executionURL,"HTTP 500")); 
		}
		return executed;
	}
	
	
	public static  Boolean invokePutExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Boolean executed = false;
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

		HttpEntity<String> entity = new HttpEntity<String>(executionRequestBody, headers);
		Object myObject;
		try{
			//ResponseEntity<String> responseString = restTemplate.exchange("http://ENGINE-SERVICE/engine/ebpmn/services/SIKOMSWrkFlow/startinstance/1", HttpMethod.POST,entity, String.class, map); 
			if(restTemplate == null){
				restTemplate = new RestTemplate(); 
			}
			ResponseEntity<String> responseString = restTemplate.exchange(executionURL, HttpMethod.PUT,entity, String.class, map); 
			if(responseString != null){
				if(responseString.getStatusCode() == HttpStatus.OK){
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					logger.debug(myObject.toString());
					
					executed = true;
				}else if(responseString.getStatusCode() == HttpStatus.CREATED){
					
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					logger.debug(myObject.toString());
					executed = true; 
				}
			}
		}catch(RestClientException e){
			logger.error("RestClientException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		}
		return executed;
	}
	
	
	
	public static Object invokeGetExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate) throws Exception{
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

		HttpEntity<String> entity = new HttpEntity<String>(executionRequestBody, headers);
		Object myObject = null; 
		try{
			//ResponseEntity<String> responseString = restTemplate.exchange("http://ENGINE-SERVICE/engine/ebpmn/services/SIKOMSWrkFlow/startinstance/1", HttpMethod.POST,entity, String.class, map); 
			if(restTemplate == null){
				restTemplate = new RestTemplate(); 
			}
			ResponseEntity<String> responseString = restTemplate.exchange(executionURL, HttpMethod.GET,entity, String.class, map); 
			if(responseString != null){
				if(responseString.getStatusCode() == HttpStatus.OK){
					myObject = responseString.getBody(); 
				}
			}
		}catch(RestClientException e){
			if(e.getMessage().contains("404")){
				logger.info(String.format("Resource at %s does not exist",executionURL));
			}else{
				logger.error("Exception invokeGetExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " cause:"+e.getMessage(), e);
			}
			if(e.getMessage().contains("404")){
				//throw new Exception(String.format("%s returned %s", executionURL,"HTTP 404")); 
			}else if(e.getMessage().contains("500")) {
				throw new Exception(String.format("%s returned %s", executionURL,"HTTP 500")); 
			}else {
				throw new Exception(String.format("%s returned %s", executionURL,e.getLocalizedMessage()));
			}
			
		} catch (Exception e) {
			logger.error("Exception invokeGetExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " cause:"+e.getMessage(), e);
			throw new Exception(String.format("%s returned %s", executionURL,e.getLocalizedMessage()));
		}
		return myObject;
	}
	
	public static Object invokePatchExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

		HttpEntity<String> entity = new HttpEntity<String>(executionRequestBody, headers);
		Object myObject = null; 
		try{
			//ResponseEntity<String> responseString = restTemplate.exchange("http://ENGINE-SERVICE/engine/ebpmn/services/SIKOMSWrkFlow/startinstance/1", HttpMethod.POST,entity, String.class, map); 
			if(restTemplate == null){
				restTemplate = new RestTemplate(); 
			}
			HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
			requestFactory.setConnectTimeout(3600);
			requestFactory.setReadTimeout(3600);

			restTemplate.setRequestFactory(requestFactory);

			ResponseEntity<String> responseString = restTemplate.exchange(executionURL, HttpMethod.PATCH,entity, String.class, map); 
			if(responseString != null){
				if(responseString.getStatusCode() == HttpStatus.OK){
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					logger.debug(myObject.toString());
					
				}else if(responseString.getStatusCode() == HttpStatus.CREATED){
					
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					logger.debug(myObject.toString());
				}else if(responseString.getStatusCode() == HttpStatus.NO_CONTENT){
					myObject = responseString.getStatusCode(); 
					logger.debug(myObject.toString());
				}
			}
		}catch(RestClientException e){
			logger.error("RestClientException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		}
		return myObject;
	}
	
	public static  Object invokePostExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

		HttpEntity<String> entity = new HttpEntity<String>(executionRequestBody, headers);
		Object myObject = null;
		try{
			//ResponseEntity<String> responseString = restTemplate.exchange("http://ENGINE-SERVICE/engine/ebpmn/services/SIKOMSWrkFlow/startinstance/1", HttpMethod.POST,entity, String.class, map); 
			if(restTemplate == null){
				restTemplate = new RestTemplate(); 
			}
			ResponseEntity<String> responseString = restTemplate.exchange(executionURL, HttpMethod.POST,entity, String.class, map); 
			if(responseString != null){
				if(responseString.getStatusCode() == HttpStatus.OK){
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					logger.debug(myObject.toString());
				}else if(responseString.getStatusCode() == HttpStatus.CREATED){
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					logger.debug(myObject.toString());
				}
			}
		}catch(RestClientException e){
			logger.error("RestClientException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage(), e);
			//e.printStackTrace();
		}
		return myObject;
	}
	/**
	 * 
	 * @return
	 */
	public static Timestamp getCurrentDate() {
		java.sql.Timestamp sqlDate = null;
		try{
			SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			df.setTimeZone(TimeZone.getTimeZone("UTC")); //set UTC time
			java.util.Date utilDate = new java.util.Date();
		    String dateString = df.format(utilDate);
		    utilDate = df.parse(dateString);
		    sqlDate = new java.sql.Timestamp(utilDate.getTime());
		    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));		
			return sqlDate;
		}catch(Exception e){
			logger.error("Error on getting date :"+ e.getMessage(), e);
			return sqlDate;
		}
	}
	/**
	 * Converts a millisecond string into a UTC Date Object
	 * @param target
	 * @return
	 */
	public static java.util.Date MillisSecondStringToDate(String target){
		long milliSeconds= Long.parseLong(target);
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(milliSeconds);
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		formatter.setCalendar(calendar);
		return formatter.getCalendar().getTime(); 	
	}
	
	
	/**
	 * Returns the response body with any object mapping
	 * @param executionURL
	 * @param executionRequestBody
	 * @param restTemplate
	 * @return
	 * @throws Exception 
	 */
	public static  Object invokePostExecution2(String executionURL,String executionRequestBody,RestTemplate restTemplate) throws Exception{
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON_UTF8);

		HttpEntity<String> entity = new HttpEntity<String>(executionRequestBody, headers);
		Object myObject = null;
		try{
			//ResponseEntity<String> responseString = restTemplate.exchange("http://ENGINE-SERVICE/engine/ebpmn/services/SIKOMSWrkFlow/startinstance/1", HttpMethod.POST,entity, String.class, map); 
			if(restTemplate == null){
				restTemplate = new RestTemplate(); 
			}
			ResponseEntity<String> responseString = restTemplate.exchange(executionURL, HttpMethod.POST,entity, String.class, map); 
			if(responseString != null){
				if(responseString.getStatusCode() == HttpStatus.OK){
					myObject = responseString.getBody(); 
					//logger.debug(responseString.getBody());
				}else if(responseString.getStatusCode() == HttpStatus.CREATED){
					myObject = responseString.getBody(); 
					//logger.debug(responseString.getBody());
				}
			}
		}catch(RestClientException e){
		
			if(e.getMessage().contains("404")){
				logger.debug("RestClientException invokeExecution (DMN)  >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " cause:"+e.getMessage(), e);
				logger.debug(executionRequestBody);
			}else if(e.getMessage().contains("500")) {
				logger.error("RestClientException invokeExecution (DMN)  >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " cause:"+e.getMessage(), e);
				logger.debug(executionRequestBody);
			}else {
				logger.error("RestClientException invokeExecution (DMN)  >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " cause:"+e.getMessage(), e);
				logger.debug(executionRequestBody);
			}
		} 
		return myObject;
	}
	
	
	public static void sendToEventQueue(String inputMessage) {
		Map<String, Object> props = new HashMap<>();
		
		if(!EventstoreApplication.PLATFORM_USE_WRITE_EVENT_QUEUE) {
			logger.warn("PLATFORM_USE_WRITE_EVENT_QUEUE is set to false no event message were written to the event queue");
			return; 
		}
		
		if(EventstoreApplication.PLATFORM_KAFKA_CLUSTER == null || EventstoreApplication.PLATFORM_KAFKA_CLUSTER.isEmpty()) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_HOST+":"+EventstoreApplication.PLATFORM_KAFKA_PORT);
			logger.error(String.format("sendToEventQueue Topic %s non cluster %s:%s", EventstoreApplication.PLATFORM_KAFKA_TOPIC,
					EventstoreApplication.PLATFORM_KAFKA_HOST,EventstoreApplication.PLATFORM_KAFKA_PORT));
		}else {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_CLUSTER);
			logger.error(String.format("sendToEventQueue Topic %s Cluster %s", EventstoreApplication.PLATFORM_KAFKA_TOPIC,EventstoreApplication.PLATFORM_KAFKA_CLUSTER));
		}
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
	
		try {
			Producer<String, String> producer = new KafkaProducer<>(props);
			logger.error(String.format("sendToEventQueue %s", inputMessage ));		
			producer.send(new ProducerRecord<String, String>(EventstoreApplication.PLATFORM_KAFKA_TOPIC,inputMessage));
	        producer.close();
		} catch (Exception e) {
			logger.error("Exception on sendToProcessQueue "+e.getMessage(), e);
		}
		
	}
	
	/**
	 * 
	 * @param inputMessage
	 * @param retryId
	 */
	public static void sendToEventQueueRetry(String inputMessage,Integer retryId) {
		Map<String, Object> props = new HashMap<>();
		
		if(!EventstoreApplication.PLATFORM_USE_WRITE_EVENT_QUEUE) {
			logger.warn("PLATFORM_USE_WRITE_EVENT_QUEUE is set to false no event message were written to the event queue");
			return; 
		}
		
		if(EventstoreApplication.PLATFORM_KAFKA_CLUSTER == null || EventstoreApplication.PLATFORM_KAFKA_CLUSTER.isEmpty()) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_HOST+":"+EventstoreApplication.PLATFORM_KAFKA_PORT);
		}else {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_CLUSTER);
		}
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		String topic = String.format("%sRetry%d", EventstoreApplication.PLATFORM_KAFKA_TOPIC,retryId);
		try {
			Producer<String, String> producer = new KafkaProducer<>(props);
			
			producer.send(new ProducerRecord<String, String>(topic,inputMessage));
	        producer.close();
		} catch (Exception e) {
			logger.error("Exception on writing to "+topic +" "+e.getMessage(), e);
		}
		
	}
	
	public static void sendToEventQueueFallOut(String inputMessage) {
		Map<String, Object> props = new HashMap<>();
		
		if(!EventstoreApplication.PLATFORM_USE_WRITE_EVENT_QUEUE) {
			logger.warn("PLATFORM_USE_WRITE_EVENT_QUEUE is set to false no event message were written to the event queue");
			return; 
		}
		
		if(EventstoreApplication.PLATFORM_KAFKA_CLUSTER == null || EventstoreApplication.PLATFORM_KAFKA_CLUSTER.isEmpty()) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_HOST+":"+EventstoreApplication.PLATFORM_KAFKA_PORT);
		}else {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_CLUSTER);
		}
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
	
		try {
			Producer<String, String> producer = new KafkaProducer<>(props);
			String topic = String.format("%sFALLOUT", EventstoreApplication.PLATFORM_KAFKA_TOPIC);
			producer.send(new ProducerRecord<String, String>(topic,inputMessage));
	        producer.close();
		} catch (Exception e) {
			logger.error("Exception on sendToProcessQueue "+e.getMessage(), e);
		}
		
	}
	
	
	public static void sendToPushQueue(String inputMessage) {
		Map<String, Object> props = new HashMap<>();
		
		if(!EventstoreApplication.PLATFORM_USE_WRITE_EVENT_QUEUE) {
			logger.warn("PLATFORM_USE_WRITE_EVENT_QUEUE is set to false no event message were written to the event queue");
			return; 
		}
		
		if(EventstoreApplication.PLATFORM_KAFKA_CLUSTER == null || EventstoreApplication.PLATFORM_KAFKA_CLUSTER.isEmpty()) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_HOST+":"+EventstoreApplication.PLATFORM_KAFKA_PORT);
		}else {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_CLUSTER);
		}
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
	
		try {
			Producer<String, String> producer = new KafkaProducer<>(props);
			producer.send(new ProducerRecord<String, String>(EventstoreApplication.PUSHQUEUE,inputMessage));
	        producer.close();
		} catch (Exception e) {
			logger.error("Exception on sendToProcessQueue "+e.getMessage(), e);
		}
		
	}
	
	/**
	 * Producer for CDCQueue Topic
	 * @param inputMessage
	 * 
	 */
	
	public static void sendToCDCQueue(String inputMessage) {
		Map<String, Object> props = new HashMap<>();
		
		if(!EventstoreApplication.PLATFORM_USE_WRITE_EVENT_QUEUE) {
			logger.warn("PLATFORM_USE_WRITE_EVENT_QUEUE is set to false no event message were written to the event queue");
			return; 
		}
		
		if(EventstoreApplication.PLATFORM_KAFKA_CLUSTER == null || EventstoreApplication.PLATFORM_KAFKA_CLUSTER.isEmpty()) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_HOST+":"+EventstoreApplication.PLATFORM_KAFKA_PORT);
		}else {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_CLUSTER);
		}
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
	
		try {
			Producer<String, String> producer = new KafkaProducer<>(props);
			producer.send(new ProducerRecord<String, String>(EventstoreApplication.PLATFORM_KAFKA_CDCTOPIC,inputMessage));
	        producer.close();
		} catch (Exception e) {
			logger.error("Exception on sendToCDCQueue "+e.getMessage(), e);
		}
		
	}
	
	public static boolean isNotificationMSAvailable() {
		boolean available = false; 
		String DMNURL = EventstoreApplication.prop.getProperty("DMNURL"); 
		String DAASURL = EventstoreApplication.prop.getProperty("DAASURL"); 
		//String URL = EventstoreApplication.prop.getProperty("PUSHNOTIFICATIONURL"); 
		
		try {
			//Object notification = CommonMethods.invokeGetExecution(URL,"{}", new RestTemplate());
			Object dmn = CommonMethods.invokeGetExecution(DMNURL,"{}", new RestTemplate());
			Object polyglot = CommonMethods.invokeGetExecution(DAASURL,"{}", new RestTemplate());
			
			//if(notification != null && dmn != null && polyglot != null) available = true; 
			if(dmn != null && polyglot != null) available = true; 
			if(!available) Thread.sleep(500);
		}catch(Exception ex) {
			logger.error(ex.getLocalizedMessage(), ex);
		}
		
		return available; 
		
	}
	/***
	 * 
	 * @return
	 */
	public static String getKafkaBootStrap() {
		String bootStrapServerConfig = EventstoreApplication.PLATFORM_KAFKA_CLUSTER; 
		
		if(EventstoreApplication.PLATFORM_KAFKA_CLUSTER == null || EventstoreApplication.PLATFORM_KAFKA_CLUSTER.isEmpty()) 
			bootStrapServerConfig = EventstoreApplication.PLATFORM_KAFKA_HOST+":"+EventstoreApplication.PLATFORM_KAFKA_PORT;
		
		return bootStrapServerConfig; 
	}
	
	/**
	 * 
	 * @param s
	 * @param offset
	 * @param errorMessage
	 */
public static void StreamExceptionHandler(String s,long offset,String errorMessage) {
		
		try {
			JsonParser jsonParser = new JsonParser();
			JsonElement dmnTree = jsonParser.parse(s); 
			JsonObject message = dmnTree.getAsJsonObject();
			logger.error(String.format("<<>> Send to EventQueueFallout cause %s >>>  offset = %d, value = %s ",errorMessage,offset, s));
			CommonMethods.sendToEventQueueFallOut(message.toString());
		
		}catch(Exception ex) {
			logger.error(String.format("%s %s %d", ex.getLocalizedMessage(),s,offset), ex);
		}
		
	}

public static void reSyncRetry(Integer retryId) {
	final OffsetStorage db = new OffsetStorage();
	String key; 
	if(retryId == -1 ) {
		key = String.format("%s.%s", EventstoreApplication.PLATFORM_KAFKA_TOPIC,EventstoreApplication.PLATFORM_KAFKA_GROUP);
	}else {
		key = String.format("%sRetry%d.%s", EventstoreApplication.PLATFORM_KAFKA_TOPIC,retryId,EventstoreApplication.PLATFORM_KAFKA_GROUP);
	}
	if(EventstoreApplication.getCacheBPMN().exist(key)) {
		Map<String, String> offSetMsg = EventstoreApplication.getCacheBPMN().getAll(key, 80); 
		offSetMsg.forEach((x,y)->{
			String Id = x; 
			JsonParser jsonParser = new JsonParser();
			JsonElement dmnTree = jsonParser.parse(y); 
			JsonObject tb = dmnTree.getAsJsonObject();
			db.updateExternalOffSet(tb.get("groupname").getAsString(),
					tb.get("topicname").getAsString(), 
					tb.get("partitionid").getAsInt(),
					tb.get("offset").getAsLong(), 
					Id);
			
			
		});
	}
}


public static void sendToCDCQueueFailOut(String inputMessage) {
	Map<String, Object> props = new HashMap<>();
	
	if(!EventstoreApplication.PLATFORM_USE_WRITE_EVENT_QUEUE) {
		logger.warn("PLATFORM_USE_WRITE_EVENT_QUEUE is set to false no event message were written to the event queue");
		return; 
	}
	
	if(EventstoreApplication.PLATFORM_KAFKA_CLUSTER == null || EventstoreApplication.PLATFORM_KAFKA_CLUSTER.isEmpty()) {
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_HOST+":"+EventstoreApplication.PLATFORM_KAFKA_PORT);
	}else {
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EventstoreApplication.PLATFORM_KAFKA_CLUSTER);
	}
	props.put(ProducerConfig.RETRIES_CONFIG, 0);
	props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
	props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
	props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	

	try {
		Producer<String, String> producer = new KafkaProducer<>(props);
		String topic = String.format("%sFALLOUT", EventstoreApplication.PLATFORM_KAFKA_CDCTOPIC);
		producer.send(new ProducerRecord<String, String>(topic,inputMessage));
        producer.close();
	} catch (Exception e) {
		logger.error("Exception on sendToCDCQueueFailOut "+e.getMessage(), e);
	}
	
}




}



