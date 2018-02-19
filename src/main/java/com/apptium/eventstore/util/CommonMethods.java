package com.apptium.eventstore.util;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CommonMethods {
	//static final Logger logger = LogManager.getLogger(CommonMethods.class);
	
	static final Logger logger = LoggerFactory.getLogger(CommonMethods.class);
	
	public static  Boolean invokeExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Boolean executed = false;
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON);

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
					logger.debug(myObject.toString());
					
					executed = true;
				}else if(responseString.getStatusCode() == HttpStatus.CREATED){
					
					myObject = new ObjectMapper().readValue(responseString.getBody(),Object.class);
					logger.debug(myObject.toString());
					executed = true; 
				}
			}
		}catch(RestClientException e){
			logger.error("RestClientException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody);
			//e.printStackTrace();
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody);
			//e.printStackTrace();
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody);
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " requestBody: "+executionRequestBody);
			//e.printStackTrace();
		}
		return executed;
	}
	
	
	public static  Boolean invokePutExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Boolean executed = false;
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON);

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
			logger.error("RestClientException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		}
		return executed;
	}
	
	
	
	public static Object invokeGetExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON);

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
				logger.error("Exception invokeGetExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " cause:"+e.getMessage());
			}
			
		} catch (Exception e) {
			logger.error("Exception invokeGetExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionURL + " cause:"+e.getMessage());
		}
		return myObject;
	}
	
	public static Object invokePatchExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON);

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
			logger.error("RestClientException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException invokePatchExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		}
		return myObject;
	}
	
	public static  Object invokePostExecution(String executionURL,String executionRequestBody,RestTemplate restTemplate){
		Map<String, String> map = new HashMap<String, String>();
		HttpHeaders headers = new HttpHeaders(); 
		headers.setContentType(MediaType.APPLICATION_JSON);

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
			logger.error("RestClientException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (JsonParseException e) {
			logger.error("JsonParseException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException invokeExecution >>>>> "+String.valueOf(e.getMessage()) + " URL "+ executionRequestBody + " cause:"+e.getMessage());
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
			logger.error("Error on getting date :"+ e.getMessage());
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
}
