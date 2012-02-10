package org.hibernate.ogm.datastore.mapbased.impl;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;
import org.hibernate.cfg.Environment;
import org.hibernate.ogm.datastore.spi.AbstractMapBasedDatastoreProvider;
import org.hibernate.ogm.grid.EntityKey;
import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * 
 * @author seiyak <skawashima@uchicago.edu>
 * 
 */
public class RedisDatastoreProvider extends AbstractMapBasedDatastoreProvider {

	Logger log = Logger.getLogger(RedisDatastoreProvider.class);
	private JedisPool pool = null;
	private Jedis jedis = null;

	private static enum KeyWords {

		SET("set"), PERIOD("."), VALUE_PREFIX("Value"), REDIS_CONFIG_PREFIX(
				"hibernate.datastore.provider.redis_config."), ASTERISK("*");

		private String keyWord;

		KeyWords(String keyWord) {
			this.keyWord = keyWord;
		}

		public String getKeyWord() {
			return this.keyWord;
		}
	}

	@Override
	public void start() {
		log.info("Redis starting ...");
		this.setUpRedis();
	}

	/**
	 * Sets up Redis.
	 */
	private void setUpRedis() {
		this.pool = new JedisPool(this.createJedisConfig(), this
				.getRequiredPropertyValues().get("provider_url"));
		this.jedis = this.pool.getResource();
	}

	/**
	 * Reads Redis configuration properties from hibernate.properties and
	 * creates JedisPoolConfig object.
	 * 
	 * @return Newly created JedisPoolConfig object.
	 */
	private final JedisPoolConfig createJedisConfig() {

		final JedisPoolConfig jedisConfig = new JedisPoolConfig();

		Set<Entry<Object, Object>> props = Environment.getProperties()
				.entrySet();
		for (Iterator<Entry<Object, Object>> itr = props.iterator(); itr
				.hasNext();) {

			Entry<Object, Object> prop = itr.next();
			String key = (String) prop.getKey();
			if (key.startsWith(KeyWords.REDIS_CONFIG_PREFIX.getKeyWord())) {
				this.checkConfigPropAgainstConfigMethod(key,
						(String) prop.getValue(), jedisConfig);
			}
		}

		return jedisConfig;
	}

	/**
	 * Checks if the specified key is corresponding to the method on
	 * JedisPoolConfig.
	 * 
	 * @param key
	 *            The specified property name to be used to call the
	 *            corresponding config method.
	 * @param value
	 *            The specified property value to be set.
	 * @param jedisConfig
	 *            JedisPoolConfig object where the property is set calling
	 *            corresponding setter method.
	 * @return True if the corresponding method exists, false otherwise.
	 */
	private boolean checkConfigPropAgainstConfigMethod(String key,
			String value, JedisPoolConfig jedisConfig) {

		String keySubStr = key.substring(key.lastIndexOf(KeyWords.PERIOD
				.getKeyWord()) + 1);
		Method[] methods = jedisConfig.getClass().getDeclaredMethods();
		for (Method method : methods) {
			if (method.getName().startsWith(KeyWords.SET.getKeyWord())
					&& method.getName().substring(3).equals(keySubStr)) {
				this.setConfigProp(jedisConfig, method, value);
				return true;
			}
		}

		return false;
	}

	/**
	 * Calls the specified method with the specified value casted when required
	 * on the specified JedisPoolConfig object. This method expects the number
	 * of the parameters is 1 since this is a setter method.
	 * 
	 * @param jedisConfig
	 *            Object where the method is invoked.
	 * @param method
	 *            Method to be invoked.
	 * @param value
	 *            Value to be set invoking the specified method.
	 */
	private void setConfigProp(JedisPoolConfig jedisConfig, Method method,
			String value) {

		Class[] paramClasses = method.getParameterTypes();
		try {
			Object targetObj = this.createWrapperClassObjFrom(paramClasses[0],
					value);
			method.invoke(
					jedisConfig,
					targetObj
							.getClass()
							.getDeclaredMethod(
									paramClasses[0].getCanonicalName()
											+ KeyWords.VALUE_PREFIX
													.getKeyWord())
							.invoke(targetObj));
		} catch (SecurityException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (NoSuchMethodException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IllegalArgumentException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IllegalAccessException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (InvocationTargetException e) {
			this.throwHibernateExceptionFrom(e);
		}
	}

	@Override
	public void stop() {
		log.info("stopping Redis");
		this.pool.destroy();
	}

	@Override
	protected void putEntityToDatastore(EntityKey key, Map<String, Object> tuple) {

		this.connectToServerIfNeeded();

		Map<String, Object> map = this.toJSONIfNeeded(tuple);
		log.info("about to put key: " + key.getEntityKeyAsMap() + " value: "
				+ tuple);
		
		//// TODO add rollback manually
		try {
			Transaction tx = this.jedis.multi();
			tx.set(this.toJSON(key.getEntityKeyAsMap()), map.toString());
			tx.exec();
		} finally {
			this.pool.returnResource(this.jedis);
		}
	}

	@Override
	public Map<String, Object> getEntityTupleFromDatastore(EntityKey key) {

		Map<String, Object> tuple = this.getEntityAsMap(key);
		this.createEntityTupleFromDatastore(key, tuple);
		return tuple;
	}

	@Override
	protected void removeEntityTupleFromDatastore(EntityKey key) {
		this.connectToServerIfNeeded();
		try {
			Transaction tx = this.jedis.multi();
			log.info("about to delete with key: " + key.getEntityKeyAsMap());
			Response<Long> res = tx.del(this.toJSON(key.getEntityKeyAsMap()));
			tx.exec();
		} finally {
			this.pool.returnResource(this.jedis);
		}

	}

	@Override
	protected Map<String, Object> getEntityWith(EntityKey key) {
		this.connectToServerIfNeeded();
		log.info("getting with key: " + this.toJSON(key.getEntityKeyAsMap())
				+ " entity name: " + key.getEntityName());

		boolean isNull = false;
		Response<String> res = null;
		try {
			Transaction tx = this.jedis.multi();
			log.info("entity key: " + this.toJSON(key.getEntityKeyAsMap()));
			res = tx.get(this.toJSON(key.getEntityKeyAsMap()));
			tx.exec();
		} catch (NullPointerException ex) {
			log.warn("there is no corresponding value to the specified key yet: "
					+ key);
			isNull = true;
		} finally {
			this.pool.returnResource(this.jedis);
		}

		if (isNull) {
			return null;
		}

		log.info("get from Redis: " + res.get());
		return this.getMapFromJSONString(res.get());
	}

	@Override
	protected boolean checkRequiredSettings() {
		if (this.getRequiredPropertyValues().get("provider")
				.equals(this.getClass().getCanonicalName())
				&& this.getRequiredPropertyValues().get("grid_dialect")
						.equals(this.getDefaultDialect().getCanonicalName())
				&& this.getRequiredPropertyValues().get("dialect")
						.equals("org.hibernate.ogm.dialect.RedisDialect")) {
			return true;
		}

		return false;
	}

	@Override
	protected void connectToServerIfNeeded() {
		this.setUpRedis();
	}

	@Override
	public Map<EntityKey, Map<String, Object>> getEntityMap() {

		Map<EntityKey, Map<String, Object>> map = new HashMap<EntityKey, Map<String, Object>>();
		try {
			Transaction tx = this.jedis.multi();
			log.info("getting all keys");
			Response<Set<String>> keys = tx
					.keys(KeyWords.ASTERISK.getKeyWord());
			tx.exec();

			for (Iterator<String> itr = keys.get().iterator(); itr.hasNext();) {
				String key = itr.next();
				log.info("key: " + key);
				JSONObject entityKeyJson = null;
				try {
					entityKeyJson = new JSONObject(key);
				} catch (JSONException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				EntityKey entityKey = null;
				try {
					entityKey = new EntityKey(
							(String) entityKeyJson.get("table"),
							(Serializable) entityKeyJson.get("id"));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				log.info("EntityKey object: " + entityKey);
				log.info("EntityKey json: "
						+ this.toJSON(entityKey.getEntityKeyAsMap()));

				tx = this.jedis.multi();
				Response<String> value = tx.get(this.toJSON(entityKey
						.getEntityKeyAsMap()));
				tx.exec();
				try {
					log.info("value: " + value.get()
							+ " corresponding to the key: "
							+ this.toJSON(entityKey.getEntityKeyAsMap()));
					Map<String, Object> m = this.getMapFromJSONString(value
							.get());
					map.put(entityKey, m);
				} catch (NullPointerException ex) {
					ex.printStackTrace();
				}
			}
		} finally {
			this.pool.returnResource(this.jedis);
		}

		return map;
	}
}
