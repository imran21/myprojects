package com.apptium.eventstore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.apptium.eventstore.util.AsyncThread;

@Configuration
@SpringBootApplication
@EnableAutoConfiguration
public class EventstoreApplication {
	public static Environment prop;
	public static ApplicationContext ctx = null; 
	public static boolean PLATFORM_USE_WRITE_EVENT_QUEUE = false;
	
	public static String PLATFORM_KAFKA_HOST;
	public static String PLATFORM_KAFKA_PORT;
	public static String PLATFORM_KAFKA_TOPIC;
	public static String PLATFORM_KAFKA_CLUSTER; 
	public static String PLATFORM_KAFKA_GROUP; 
	public static int PLATFORM_KAFKA_LOOPCOUNT; 
	public static int PLATFORM_KAFKA_POLLTIME; 
	
	public static void main(String[] args) {
		 ctx = SpringApplication.run(EventstoreApplication.class, args);
		 prop = ctx.getBean(Environment.class);
		 
		 
			PLATFORM_KAFKA_HOST = prop.getProperty("PLATFORM_KAFKA_HOST") != null ? prop.getProperty("PLATFORM_KAFKA_HOST") : null;
			
			PLATFORM_KAFKA_PORT = prop.getProperty("PLATFORM_KAFKA_PORT") != null ? prop.getProperty("PLATFORM_KAFKA_PORT") : null;
			
			PLATFORM_KAFKA_TOPIC = prop.getProperty("PLATFORM_KAFKA_TOPIC") != null ? prop.getProperty("PLATFORM_KAFKA_TOPIC") : null;
			
			PLATFORM_KAFKA_CLUSTER = prop.getProperty("PLATFORM_KAFKA_CLUSTER") != null ? prop.getProperty("PLATFORM_KAFKA_CLUSTER") : null;
				
			PLATFORM_USE_WRITE_EVENT_QUEUE = prop.getProperty("PLATFORM_USE_WRITE_EVENT_QUEUE") != null ? Boolean.valueOf(prop.getProperty("PLATFORM_USE_WRITE_EVENT_QUEUE")) : true; 
			
			PLATFORM_KAFKA_GROUP = prop.getProperty("PLATFORM_KAFKA_GROUP") != null ? prop.getProperty("PLATFORM_KAFKA_GROUP") : "GROUP2";
			
			PLATFORM_KAFKA_LOOPCOUNT = prop.getProperty("PLATFORM_KAFKA_LOOPCOUNT") != null ? Integer.parseInt(prop.getProperty("PLATFORM_KAFKA_LOOPCOUNT").toString()) : 10;
			
			PLATFORM_KAFKA_POLLTIME = prop.getProperty("PLATFORM_KAFKA_POLLTIME") != null ?  Integer.parseInt(prop.getProperty("PLATFORM_KAFKA_POLLTIME").toString()) : 5000;
			
			
			if(PLATFORM_USE_WRITE_EVENT_QUEUE) {
				Runnable r = new AsyncThread();
				new Thread(r).start();
			}
	

		}
 }
