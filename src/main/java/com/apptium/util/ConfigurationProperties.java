package com.apptium.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.env.Environment;

import com.apptium.EventstoreApplication;

public class ConfigurationProperties {
	private static final Log logger = LogFactory.getLog(ConfigurationProperties.class);


	private static	Environment prop;

	public static String getConfigValue(String key) throws IOException {
		String value = null;
		if (prop == null) {
			prop = EventstoreApplication.ctx.getBean(Environment.class);
		}
		if (prop.getProperty(key) != null) {
			value = prop.getProperty(key).trim();
		}
		if (value == null && System.getenv(key) !=null) {
			value = System.getenv(key).trim();
		}
		return value;
	}
	public int getConfigValueAsInteger(String key)   {
		String value = null;
		if (prop == null) {
			prop = EventstoreApplication.ctx.getBean(Environment.class);
		}
		if (prop.getProperty(key) != null) {
			value = prop.getProperty(key).trim();
		}
		if (value == null && System.getenv(key) !=null) {
			value = System.getenv(key).trim();
		}
		return Integer.parseInt(value);
	}

}