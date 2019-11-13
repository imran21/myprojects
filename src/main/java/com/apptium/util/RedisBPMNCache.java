package com.apptium.util;

import argo.jdom.JdomParser;
import argo.jdom.JsonNode;
import argo.saj.InvalidSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/***
 * 
 * @author taftwallaceiii
 *
 */
public class RedisBPMNCache {
	final Logger LOG = LoggerFactory.getLogger(RedisBPMNCache.class);

	private static JedisPool jedisPool = null;
	private static JedisCluster jedisCluster = null;
	private static JedisSentinelPool jedisSentinelPool = null; 
	private static String CF_REDIS_SENTINELS_MASTER = null;
	private static String CF_REDIS_SENTINELS = null;
	private static Boolean CF_USE_REDIS_SENTINELS = false;
	
	private final int JOBDBINDEX = 0; 
	private final String JOBKEY = "TOPICOFFSET"; 
	
    boolean useCluster=false; 


	/**
	 * Read all the decision nodes from the neo4j and load them to cache.
	 * 
	 * "VCAP_SERVICES": {
      "p-redis": [
        {
          "name": "acela-ce-app-redis",
          "label": "p-redis",
          "tags": [
            "pivotal",
            "redis"
          ],
          "plan": "shared-vm",
          "credentials": {
            "host": "10.251.127.51",
            "password": "cbea4271-ad64-45b3-bfab-50dc7318abc8",
            "port": 41361
          }
        }
      ],
	 */
	public RedisBPMNCache(boolean useRedis) {
		
		try {
			CF_USE_REDIS_SENTINELS = ConfigurationProperties.getConfigValue("CF_USE_REDIS_SENTINELS") != null 
					? Boolean.valueOf(ConfigurationProperties.getConfigValue("CF_USE_REDIS_SENTINELS")) : false; 
					
			CF_REDIS_SENTINELS_MASTER = ConfigurationProperties.getConfigValue("CF_REDIS_SENTINELS_MASTER") != null 
						? ConfigurationProperties.getConfigValue("CF_REDIS_SENTINELS_MASTER") : null; 	
						
			CF_REDIS_SENTINELS = ConfigurationProperties.getConfigValue("CF_REDIS_SENTINELS") != null 
								? ConfigurationProperties.getConfigValue("CF_REDIS_SENTINELS") : null; 				
		}catch(Exception e) {
			LOG.error(e.getLocalizedMessage(), e);
		}
		
		if ( useRedis ) {
			try {
				JedisPoolConfig poolConfig = new JedisPoolConfig();
		        poolConfig.setTestOnBorrow(true);
		        poolConfig.setTestOnReturn(true);
		        poolConfig.setTestWhileIdle(true);
		        poolConfig.setNumTestsPerEvictionRun(50);
		        poolConfig.setMinEvictableIdleTimeMillis(60000);
		        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
		        poolConfig.setMaxTotal(2000);
		        poolConfig.setMaxIdle(150);
		        poolConfig.setMinIdle(100);
			    
			    /**
			     * TW 7-18-2018 Added logic to evaluate the VCAP_SERVICES JSON
			     * if it has the field key p-redis then attempt to bind to the CF service
			     * otherwise using the environment variables to bind to external REDIS
			     */
			    String vcap_services = ConfigurationProperties.getConfigValue("VCAP_SERVICES");
			    JsonNode rootCapTest = null; 
			    Boolean capTest = false; 
			    if (vcap_services != null && vcap_services.length() > 0) { 
			    	rootCapTest = new JdomParser().parse(vcap_services);
			    	capTest = rootCapTest.isNode("p-redis");
			    }
			    
			    if (vcap_services != null && capTest) {
			        JsonNode root = new JdomParser().parse(vcap_services);
			        JsonNode rediscloudNode = root.getNode("p-redis");
			        JsonNode credentials = rediscloudNode.getNode(0).getNode("credentials");
	
					init(credentials.getStringValue("host"),
			                Integer.parseInt(credentials.getNumberValue("port")),
			                Protocol.DEFAULT_TIMEOUT,
			                credentials.getStringValue("password"),poolConfig);
			    }else if(ConfigurationProperties.getConfigValue("CF_REDIS_HOST") != null &&
			    		ConfigurationProperties.getConfigValue("CF_REDIS_PASSWORD") != null  && !CF_USE_REDIS_SENTINELS){
			    	
			    	init(ConfigurationProperties.getConfigValue("CF_REDIS_HOST").toString(),
			    		Integer.parseInt(ConfigurationProperties.getConfigValue("CF_REDIS_PORT").toString()),
			    		Protocol.DEFAULT_TIMEOUT,
			    		ConfigurationProperties.getConfigValue("CF_REDIS_PASSWORD").toString(),poolConfig); 
			    	
			    }else if(ConfigurationProperties.getConfigValue("CF_REDIS_HOST") != null && !CF_USE_REDIS_SENTINELS){
			    	
			    	init(ConfigurationProperties.getConfigValue("CF_REDIS_HOST").toString(),
			    		Integer.parseInt(ConfigurationProperties.getConfigValue("CF_REDIS_PORT").toString()),
			    		Protocol.DEFAULT_TIMEOUT,
			    		null,poolConfig); 
			    	
			    }else if(!CF_USE_REDIS_SENTINELS) {
			    		jedisPool = new JedisPool(poolConfig,"localhost",6379,60000); 
			    }else if(CF_USE_REDIS_SENTINELS) {
			    	
			    		if(CF_REDIS_SENTINELS_MASTER == null) throw new Exception("No Master Name provided for REDIS Sentinels Cluster, check CF_REDIS_SENTINELS_MASTER"); 
					if(CF_REDIS_SENTINELS == null) throw new Exception("No Hosts provided for REDIS Sentinels Cluster, check CF_REDIS_SENTINELS"); 
					
					Set<String> sentinels = new HashSet<String>(Arrays.asList(CF_REDIS_SENTINELS.split(",")));
					if(ConfigurationProperties.getConfigValue("CF_REDIS_PASSWORD") != null) {
						 jedisSentinelPool = new JedisSentinelPool(CF_REDIS_SENTINELS_MASTER, sentinels,poolConfig,60000,
								 ConfigurationProperties.getConfigValue("CF_REDIS_PASSWORD")); 
					}else {
					  jedisSentinelPool = new JedisSentinelPool(CF_REDIS_SENTINELS_MASTER, sentinels,poolConfig,60000); 
					}
			    	
			    }
			} catch (InvalidSyntaxException ex) {
			    // vcap_services could not be parsed.
				LOG.error("Exception on RedisBPMNCache "+ex.getLocalizedMessage(), ex);
			} catch (Exception e) {
				LOG.error("Exception on RedisBPMNCache "+e.getLocalizedMessage(), e);
			}
		} else {
			
		}
	}
	
	private static void init(String host, int port, int timeout, String password,JedisPoolConfig poolConfig ) {
		if(password != null) {
			jedisPool = new JedisPool(poolConfig,host,port,timeout,password);
		}else {
			jedisPool = new JedisPool(poolConfig,host,port,timeout);
		}
	}
	
	private Jedis getJedis() {
		if(jedisPool != null) {
			return jedisPool.getResource();
		}else {
			return jedisSentinelPool.getResource();
		}
	}
	
	private Boolean poolObjectIsNull() {
		if(jedisPool == null && jedisSentinelPool == null) return true; 
		return false; 
	}
	
	public boolean exist(String key){
		Jedis jedis = null;
		boolean result = false; 
		
		if(poolObjectIsNull()) return result; 
		
		try {
			jedis = getJedis(); 
			result = jedis.exists(key); 
			
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return result;
	}
	
	/**
	 * Determine if a key exists in a given database
	 * @param key  The unique value 
	 * @param DBIndex The numeric database index
	 * @return True if the key exists otherwise false
	 */
	public boolean exist(String key,int DBIndex){
		Jedis jedis = null;
		boolean result = false; 
		
		if(poolObjectIsNull()) return result;
		
		try {
			jedis = getJedis(); 
			jedis.select(DBIndex); 
			result = jedis.exists(key); 
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return result;
	}
	/**
	 * Determine if a parent and child key exists in a given database
	 * @param parentKey The unique parent key value
	 * @param key The unique child key value
	 * @param DBIndex  The numeric database index
	 * @return True if the key exist otherwise false
	 */
	public boolean exist(String parentKey, String key, int DBIndex){
		Jedis jedis = null;
		boolean result = false; 
		
		if(poolObjectIsNull()) return result;
		
		try {
			jedis = getJedis(); 
			jedis.select(DBIndex); 
			result = jedis.hexists(parentKey, key); 
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return result;
	}
	/**
	 * 
	 * @param key
	 * @param fieldName
	 * @param value
	 * @param DBIndex
	 */
	public void putDB(String key,String fieldName,String value,int DBIndex){
		Jedis jedis = null;
		if(poolObjectIsNull()) return; 
		try {
			jedis =  getJedis(); 
			jedis.select(DBIndex); 
			jedis.hset(key, fieldName, value);
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
	}
	public void put(String key,String fieldName,String value,int TTL){
		Jedis jedis = null;
		if(poolObjectIsNull()) return;
		try {
			jedis = getJedis(); 
			jedis.hset(key, fieldName, value);
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
	}
	
	public String get(String key, String fieldName, int TTL){
	
		Jedis jedis = null;
		String value = null; 
		if(poolObjectIsNull()) return null;
		try {
			jedis = getJedis(); 
			if(jedis.hexists(key, fieldName)){
				value = jedis.hget(key, fieldName); 
			}
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return value;
	}
	
	public String get(String key){
		
		Jedis jedis = null;
		String value = null; 
		if(poolObjectIsNull()) return null;
		try {
			jedis = getJedis(); 
			if(jedis.exists(key)){
				value = jedis.get(key); 
			}
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return value;
	}
	
	public String getDB(String key, String fieldName,int DBIndex){
		
		Jedis jedis = null;
		String value = null; 
		if(poolObjectIsNull()) return null;
		try {
			jedis = getJedis(); 
			jedis.select(DBIndex); 
			if(jedis.hexists(key, fieldName)){
				value = jedis.hget(key, fieldName); 
			}
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return value;
	}
	
	public Map<String,String> getAllDB(String ParentKey, String key, int DBIndex){
		Jedis jedis = null;
		Map<String,String> value = null; 
		if(poolObjectIsNull()) return null;
		try {
			jedis = getJedis(); 
			jedis.select(DBIndex); 
			if(jedis.exists(ParentKey)){
				value = jedis.hgetAll(ParentKey);
				
			}
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return value;
	}
	
	
	public Map<String,String> getAll(String key, int TTL){
		Jedis jedis = null;
		Map<String,String> value = null; 
		if(poolObjectIsNull()) return null;
		try {
			jedis = getJedis(); 
			if(jedis.exists(key)){
				value = jedis.hgetAll(key); 
			}
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return value;
	}
	public void setAll(String key,Map<String,String> values, int TTL){
		Jedis jedis = null;
		if(poolObjectIsNull()) return;
		try {
			jedis = getJedis(); 
			jedis.hmset(key, values); 
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
    }
	
	/**
	 * Set the key to be removed in 5 seconds
	 * @param key
	 */
	public void setExpiration(String key,int TTL){
		Jedis jedis = null;
		if(poolObjectIsNull()) return;
		try {
			jedis = getJedis(); 
			if(jedis.exists(key)){
				jedis.expire(key, TTL);
			}
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
	
	}
	
	public void remove (String key){
		Jedis jedis = null;
		if(poolObjectIsNull()) return;
		try {
			jedis = getJedis(); 
			if(jedis.exists(key)){
				jedis.del(key); 
			}
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
	}
	/**
	 * 
	 * @param Parent
	 * @param key
	 * @param DBIndex
	 */
	public void remove (String Parent, String key, int DBIndex){
		Jedis jedis = null;
		if(poolObjectIsNull()) return;
		try {
			jedis = getJedis(); 
			jedis.select(DBIndex); 
			if(jedis.hexists(Parent, key)){
				jedis.hdel(Parent, key); 
			}
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
	}
	
	public void setExpirationSeconds(String key,int TTL){
		Jedis jedis = null;
		if(poolObjectIsNull()) return;
		try {
			jedis = getJedis(); 
			if(jedis.exists(key)){
				jedis.expire(key, TTL);
			}
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
	
	}
	/**
	 * Set the key expiration for a specific key and Database
	 * @param key
	 * @param TTL
	 * @param DBIndex
	 */
	public void setExpiration(String key,int TTL,int DBIndex){
		Jedis jedis = null;
		if(poolObjectIsNull()) return;
		try {
			jedis = getJedis(); 
			jedis.select(DBIndex); 
				jedis.expire(key, TTL);

		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
	
	}
	public Set<String> getKeys(String pattern){
		Jedis jedis = null;
		Set<String> map = new HashSet<String>(); 
		if(poolObjectIsNull()) return map;
		try {
			jedis = getJedis(); 
			map = jedis.keys(pattern); 
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return map; 
	}
	
	public List<String> scan(){
		Jedis jedis = null;
		List<String> keys = new ArrayList<String>(); 
		ScanParams scanParams = new ScanParams();
		scanParams.match(JOBKEY); 
		String cur = redis.clients.jedis.ScanParams.SCAN_POINTER_START; 
		boolean cycleIsFinished = false;
		if(poolObjectIsNull()) return keys; 
		try {
			jedis = getJedis(); 
			jedis.select(JOBDBINDEX); 
			while(!cycleIsFinished){
			
				ScanResult<String> scanResult = jedis.scan(cur, scanParams);	
				List<String> keyValue = scanResult.getResult();
				keys.addAll(keyValue); 
				cur = scanResult.getStringCursor();
				if (cur.equals("0")){
					cycleIsFinished = true;
				}                 
			
			}
	
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return keys; 
	}
	
	public List<String> scanHistory(){
		Jedis jedis = null;
		List<String> keys = new ArrayList<String>(); 
		ScanParams scanParams = new ScanParams();
		scanParams.match(JOBKEY); 
		String cur = redis.clients.jedis.ScanParams.SCAN_POINTER_START; 
		boolean cycleIsFinished = false;
		if(poolObjectIsNull()) return keys; 
		try {
			jedis = getJedis(); 
			jedis.select(JOBDBINDEX); 
			while(!cycleIsFinished){
			
				ScanResult<String> scanResult = jedis.scan(cur, scanParams);	
				List<String> keyValue = scanResult.getResult();
				keys.addAll(keyValue); 
				cur = scanResult.getStringCursor();
				if (cur.equals("0")){
					cycleIsFinished = true;
				}                 
			
			}
	
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		return keys; 
	}

	public void setExpiration(String key, long millis, int dBIndex) {
		Jedis jedis = null;
		if(poolObjectIsNull()) return; 
		try {
			jedis = getJedis(); 
			jedis.select(dBIndex); 
			if(jedis.exists(key)){
				jedis.expire(key, 86400);
			}
		
		} finally {
			  if (jedis != null) {
			    jedis.close();
			  }
		}
		
	}

}
