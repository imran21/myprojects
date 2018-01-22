package com.apptium.eventstore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@SpringBootApplication
@EnableAutoConfiguration
public class EventstoreApplication {
	public static Environment prop;
	public static ApplicationContext ctx = null; 
	
	public static void main(String[] args) {
		 ctx = SpringApplication.run(EventstoreApplication.class, args);
		 prop = ctx.getBean(Environment.class);
	}
}
