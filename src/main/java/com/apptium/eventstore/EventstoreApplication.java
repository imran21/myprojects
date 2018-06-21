package com.apptium.eventstore;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.apptium.eventstore.actors.AtLeastOnceWithBatchCommitExample;
import com.apptium.eventstore.util.AsyncThread;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

@Configuration
@SpringBootApplication
@EnableAutoConfiguration
public class EventstoreApplication {
	
	static final Logger LOG = LoggerFactory.getLogger(EventstoreApplication.class);

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
	
	public static  ActorSystem system = null; 

	public static Cache<String,String> dissimeninationRecords; 
	public static final int cacheTTL = 300; 
	public static Set<String> set; //= ConcurrentHashMap.newKeySet();
	
	public static void main(String[] args) {
		 ctx = SpringApplication.run(EventstoreApplication.class, args);
		 prop = ctx.getBean(Environment.class);
		 system = ActorSystem.create("eventStoreSystem");
		 
			PLATFORM_KAFKA_HOST = prop.getProperty("PLATFORM_KAFKA_HOST") != null ? prop.getProperty("PLATFORM_KAFKA_HOST") : null;
			
			PLATFORM_KAFKA_PORT = prop.getProperty("PLATFORM_KAFKA_PORT") != null ? prop.getProperty("PLATFORM_KAFKA_PORT") : null;
			
			PLATFORM_KAFKA_TOPIC = prop.getProperty("PLATFORM_KAFKA_TOPIC") != null ? prop.getProperty("PLATFORM_KAFKA_TOPIC") : null;
			
			PLATFORM_KAFKA_CLUSTER = prop.getProperty("PLATFORM_KAFKA_CLUSTER") != null ? prop.getProperty("PLATFORM_KAFKA_CLUSTER") : null;
				
			PLATFORM_USE_WRITE_EVENT_QUEUE = prop.getProperty("PLATFORM_USE_WRITE_EVENT_QUEUE") != null ? Boolean.valueOf(prop.getProperty("PLATFORM_USE_WRITE_EVENT_QUEUE")) : true; 
			
			PLATFORM_KAFKA_GROUP = prop.getProperty("PLATFORM_KAFKA_GROUP") != null ? prop.getProperty("PLATFORM_KAFKA_GROUP") : "GROUP2";
			
			PLATFORM_KAFKA_LOOPCOUNT = prop.getProperty("PLATFORM_KAFKA_LOOPCOUNT") != null ? Integer.parseInt(prop.getProperty("PLATFORM_KAFKA_LOOPCOUNT").toString()) : 10;
			
			PLATFORM_KAFKA_POLLTIME = prop.getProperty("PLATFORM_KAFKA_POLLTIME") != null ?  Integer.parseInt(prop.getProperty("PLATFORM_KAFKA_POLLTIME").toString()) : 5000;
			
			dissimeninationRecords = CacheBuilder.newBuilder().maximumSize(5000).expireAfterAccess(cacheTTL, TimeUnit.SECONDS).build();
			
//			if(PLATFORM_USE_WRITE_EVENT_QUEUE) {
//				Runnable r = new AsyncThread();
//				new Thread(r).start();
//			}
			set = ConcurrentHashMap.newKeySet();
			
			AtLeastOnceWithBatchCommitExample.main(args);
			gcRunner(); 
		}
	
	private static void gcRunner() {
		system.scheduler().schedule(
				Duration.create(500, TimeUnit.MILLISECONDS), // delay until scheduler starts
				Duration.create(30, TimeUnit.SECONDS), // polling interval
				new Runnable() {

					@Override
					public void run() {
						System.gc();
						LOG.info("Running GC");
					}
					
				},system.dispatcher());

	}
 }
