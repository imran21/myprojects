package com.apptium;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.apptium.actor.ExternalOffsetStorageExample;
import com.apptium.util.CommonMethods;
import com.apptium.util.RedisBPMNCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import akka.actor.ActorSystem;
import akka.kafka.javadsl.Consumer;
import scala.concurrent.duration.Duration;
// import org.springframework.cloud.client.discovery.DiscoveryClient;
// import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
//
// @EnableDiscoveryClient
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
	public static String PUSHQUEUE;
	public static Long RETRY_BASE_DURATION;
	public static int PLATFORM_KAFKA_PARTITION;

	public static  ActorSystem system = null;
	private static RedisBPMNCache cache = null;

	public static Cache<String,String> dissimeninationRecords;
	public static Integer RETRYLIMIT;
	public static final int cacheTTL = 300;
	//public static Set<String> set; //= ConcurrentHashMap.newKeySet();

	public static Consumer.Control control;

	public static void main(String[] args) {
		 ctx = SpringApplication.run(EventstoreApplication.class, args);
		 prop = ctx.getBean(Environment.class);
		 system = ActorSystem.create("eventStoreSystem");

			PLATFORM_KAFKA_HOST = prop.getProperty("PLATFORM_KAFKA_HOST") != null ? prop.getProperty("PLATFORM_KAFKA_HOST") : null;

			PLATFORM_KAFKA_PORT = prop.getProperty("PLATFORM_KAFKA_PORT") != null ? prop.getProperty("PLATFORM_KAFKA_PORT") : null;

			PLATFORM_KAFKA_TOPIC = prop.getProperty("PLATFORM_KAFKA_TOPIC") != null ? prop.getProperty("PLATFORM_KAFKA_TOPIC") : "EventQueue";

			PUSHQUEUE = prop.getProperty("PUSHQUEUE") != null ? prop.getProperty("PUSHQUEUE") : "PushQueue";

			PLATFORM_KAFKA_CLUSTER = prop.getProperty("PLATFORM_KAFKA_CLUSTER") != null ? prop.getProperty("PLATFORM_KAFKA_CLUSTER") : null;

			PLATFORM_USE_WRITE_EVENT_QUEUE = prop.getProperty("PLATFORM_USE_WRITE_EVENT_QUEUE") != null ? Boolean.valueOf(prop.getProperty("PLATFORM_USE_WRITE_EVENT_QUEUE")) : true;

			PLATFORM_KAFKA_GROUP = prop.getProperty("PLATFORM_KAFKA_GROUP") != null ? prop.getProperty("PLATFORM_KAFKA_GROUP") : "GROUP2";

			PLATFORM_KAFKA_LOOPCOUNT = prop.getProperty("PLATFORM_KAFKA_LOOPCOUNT") != null ? Integer.parseInt(prop.getProperty("PLATFORM_KAFKA_LOOPCOUNT").toString()) : 10;

			PLATFORM_KAFKA_POLLTIME = prop.getProperty("PLATFORM_KAFKA_POLLTIME") != null ?  Integer.parseInt(prop.getProperty("PLATFORM_KAFKA_POLLTIME").toString()) : 5000;

			RETRYLIMIT =  prop.getProperty("RETRYLIMIT") != null ?  Integer.parseInt(prop.getProperty("RETRYLIMIT").toString()) : 3;

			RETRY_BASE_DURATION =  prop.getProperty("RETRY_BASE_DURATION") != null ?  Long.parseLong(prop.getProperty("RETRY_BASE_DURATION").toString()) : 60000;

			PLATFORM_KAFKA_PARTITION = prop.getProperty("PLATFORM_KAFKA_PARTITION") != null ?  Integer.parseInt(prop.getProperty("PLATFORM_KAFKA_PARTITION").toString().trim()) : 0;

			dissimeninationRecords = CacheBuilder.newBuilder().maximumSize(5000).expireAfterAccess(cacheTTL, TimeUnit.SECONDS).build();

//			if(PLATFORM_USE_WRITE_EVENT_QUEUE) {
//				Runnable r = new AsyncThread();
//				new Thread(r).start();
//			}
			//set = ConcurrentHashMap.newKeySet();

			if(CommonMethods.isNotificationMSAvailable()) {
			//AtLeastOnceWithBatchCommitExample.main(args);


				ExternalOffsetStorageExample.main(args);

				ExternalOffsetStorageExample.startRetryConsumer(1);
				ExternalOffsetStorageExample.startRetryConsumer(2);
				ExternalOffsetStorageExample.startRetryConsumer(3);
			}
			gcRunner();
     		LOG.error("********* Application Started Successfully *********");
		}



	private static void gcRunner() {
		system.scheduler().schedule(
				Duration.create(500, TimeUnit.MILLISECONDS), // delay until scheduler starts
				Duration.create(30, TimeUnit.SECONDS), // polling interval
				new Runnable() {

					@Override
					public void run() {
						System.gc();
						LOG.debug("Running GC");
					}

				},system.dispatcher());

	}

	public static RedisBPMNCache getCacheBPMN(){
		if(cache == null) cache = new RedisBPMNCache(true);
		return cache;
	}
 }
