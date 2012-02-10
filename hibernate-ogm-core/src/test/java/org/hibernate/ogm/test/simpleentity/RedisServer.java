package org.hibernate.ogm.test.simpleentity;

import org.apache.log4j.Logger;
import org.hibernate.cfg.Environment;
import org.mortbay.log.Log;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisServer extends AbstractEmbeddedServer {

	private static final Logger log = Logger.getLogger(RedisServer.class);

	@Override
	public void start() {
		log.info("Redis is starting... ( does nothing from the test)");
	}

	@Override
	public void stop() {
		this.removeAllEntries();
	}

	@Override
	public boolean removeAllEntries() {
		try {
			log.info("about to remove all the keys");
			Jedis jedis = new Jedis("localhost");
			log.info("flushDB status code: " + jedis.flushDB());
		} catch (Exception ex) {
			log.error(ex.getCause());
			return false;
		}

		return true;
	}
}
