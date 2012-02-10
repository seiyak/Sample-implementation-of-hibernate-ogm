package org.hibernate.ogm.datastore.spi;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.persistence.PessimisticLockException;

import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.ogm.datastore.mapbased.impl.HashMapDialect;
import org.hibernate.ogm.datastore.mapbased.impl.WrapperClassDetector;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

/**
 * 
 * @author seiyak <skawashima@uchicago.edu>
 * 
 */
public abstract class AbstractMapBasedDatastoreProvider implements
		DatastoreProvider, Startable, Stoppable {

	private static final Logger log = Logger
			.getLogger(AbstractMapBasedDatastoreProvider.class);
	private final ConcurrentMap<RowKey, AtomicInteger> sequencesStorage = new ConcurrentHashMap<RowKey, AtomicInteger>();
	private final ConcurrentMap<Object, ReadWriteLock> dataLocks = new ConcurrentHashMap();
	protected final ConcurrentMap<AssociationKey, Map<RowKey, Map<String, Object>>> associationsKeyValueStorage = new ConcurrentHashMap<AssociationKey, Map<RowKey, Map<String, Object>>>();
	protected final WrapperClassDetector classDetector = new WrapperClassDetector();
	private final Gson gson = new Gson();
	protected final JSONedClassDetector jsonedDetector = new JSONedClassDetector();

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return HashMapDialect.class;
	}

	/**
	 * Connects to the datastore when needed such as the datastore object is
	 * null or the object has not started yet for some reason.
	 */
	protected abstract void connectToServerIfNeeded();

	public void putEntity(EntityKey key, Map<String, Object> tuple) {
		this.putEntityToDatastore(key, tuple);
	}

	protected abstract void putEntityToDatastore(EntityKey key,
			Map<String, Object> tuple);

	public Map<String, Object> getEntityTuple(EntityKey key) {
		return this.getEntityTupleFromDatastore(key);
	}

	public abstract Map<String, Object> getEntityTupleFromDatastore(
			EntityKey key);

	public void removeEntityTuple(EntityKey key) {
		this.removeEntityTupleFromDatastore(key);
	}

	protected abstract void removeEntityTupleFromDatastore(EntityKey key);

	public void putAssociation(AssociationKey key,
			Map<RowKey, Map<String, Object>> associationMap) {
		log.info("put association with key: " + key);
		log.info("association: " + associationMap);
		associationsKeyValueStorage.put(key, associationMap);
		log.info("association again: " + associationMap);
	}

	public Map<RowKey, Map<String, Object>> getAssociation(AssociationKey key) {
		log.info("get association with key: " + key);
		return associationsKeyValueStorage.get(key);
	}

	public void removeAssociation(AssociationKey key) {
		log.info("remove association with key: " + key);
		associationsKeyValueStorage.remove(key);
	}

	/**
	 * Gets the corresponding the value with the specified key.
	 * 
	 * @param Used
	 *            to retrieve the corresponding the value.
	 * @return Corresponding value as Map.
	 */
	protected abstract Map<String, Object> getEntityWith(EntityKey key);

	/**
	 * Creates entity tuple based on the retrieved values from the datastore.
	 * 
	 * @param key
	 *            Used to retrieve the corresponding value.
	 * @param tuple
	 *            Contains key value pairs from the datastore.
	 * @return Retrieved values with JSON modification when needed.
	 */
	protected Map<String, Object> createEntityTupleFromDatastore(EntityKey key,
			Map<String, Object> tuple) {

		if (tuple == null) {
			return null;
		}

		log.info("got: " + tuple);

		for (Field field : this.getDeclaredFieldsFrom(key.getEntityName())) {
			String columnName = key.getColumnName(field.getName());

			log.info("value: " + tuple.get(columnName) + " columnName: "
					+ columnName + " field: " + field.getName() + " type: "
					+ field.getType().getCanonicalName());
			if (tuple.get(columnName) != null) {
				this.putJSONedValueTo(field, columnName, tuple);
			} else {
				tuple.put(columnName, null);
			}
		}

		return tuple;
	}

	public int getSharedAtomicInteger(RowKey key, int initialValue,
			int increment) {
		AtomicInteger valueProposal = new AtomicInteger(initialValue);
		AtomicInteger previous = sequencesStorage.putIfAbsent(key,
				valueProposal);
		return previous == null ? initialValue : previous.addAndGet(increment);
	}

	/**
	 * Acquires a write lock on a specific key.
	 * 
	 * @param key
	 *            The key to lock
	 * @param timeout
	 *            in milliseconds; -1 means wait indefinitely, 0 means no wait.
	 */
	public void writeLock(EntityKey key, int timeout) {
		ReadWriteLock lock = getLock(key);
		Lock writeLock = lock.writeLock();
		acquireLock(key, timeout, writeLock);
	}

	/**
	 * Acquires a read lock on a specific key.
	 * 
	 * @param key
	 *            The key to lock
	 * @param timeout
	 *            in milliseconds; -1 means wait indefinitely, 0 means no wait.
	 */
	public void readLock(EntityKey key, int timeout) {
		ReadWriteLock lock = getLock(key);
		Lock readLock = lock.readLock();
		acquireLock(key, timeout, readLock);
	}

	private ReadWriteLock getLock(EntityKey key) {
		ReadWriteLock newLock = new ReentrantReadWriteLock();
		ReadWriteLock previous = dataLocks.putIfAbsent(key, newLock);
		return previous != null ? previous : newLock;
	}

	private void acquireLock(EntityKey key, int timeout, Lock writeLock) {
		try {
			if (timeout == -1) {
				writeLock.lockInterruptibly();
			} else if (timeout == 0) {
				boolean locked = writeLock.tryLock();
				if (!locked) {
					throw new PessimisticLockException("lock on key " + key
							+ " was not available");
				}
			} else {
				writeLock.tryLock(timeout, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException e) {
			throw new PessimisticLockException(
					"timed out waiting for lock on key " + key, e);
		}
		acquiredLocksPerThread.get().add(writeLock);
	}

	/**
	 * This simplistic data store only supports thread-bound transactions:
	 */
	private final ThreadLocal<Set<Lock>> acquiredLocksPerThread = new ThreadLocal<Set<Lock>>() {
		@Override
		protected Set<Lock> initialValue() {
			return new HashSet<Lock>();
		}
	};

	/**
	 * Meant to execute assertions in tests only
	 * 
	 * @return a read-only view of the map containing the relations between
	 *         entities
	 */
	public Map<AssociationKey, Map<RowKey, Map<String, Object>>> getAssociationsMap() {
		return Collections.unmodifiableMap(associationsKeyValueStorage);
	}

	/**
	 * Meant to execute assertions in tests only
	 * 
	 * @return a read-only view of the map containing the entities
	 */
	public Map<EntityKey, Map<String, Object>> getEntityMap() {
		return null;
	}

	/**
	 * Checks if the required properties on hibernate.properties are set
	 * correctly.
	 * 
	 * @return True if they are set correctly, false otherwise.
	 */
	protected abstract boolean checkRequiredSettings();

	protected Map<String, String> getRequiredPropertyValues() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("provider",
				Environment.getProperties().getProperty(
						"hibernate.ogm.datastore.provider"));
		map.put("grid_dialect",
				Environment.getProperties().getProperty(
						"hibernate.ogm.datastore.grid_dialect"));
		map.put("dialect",
				Environment.getProperties().getProperty("hibernate.dialect"));
		map.put("provider_url",
				Environment.getProperties().getProperty(
						"hibernate.ogm.datastore.provider_url"));
		return map;
	}

	/**
	 * Creates a startup error message when the required properties on
	 * hibernate.properties are not set correctly.
	 * 
	 * @return Error message for the situation.
	 */
	protected String createStartUpErrorMessage() {
		StringBuilder builder = new StringBuilder("Required properties for "
				+ this.getClass().getSimpleName()
				+ " are not correctly set. Please check hibernate.properties.");
		return builder.toString();
	}

	/**
	 * Converts the specified exception to HibernateException and rethrows it.
	 * 
	 * @param <T>
	 * @param exception
	 *            Exception to be rethrown as HibernateException.
	 */
	protected <T extends Throwable> void throwHibernateExceptionFrom(T exception) {
		throw new HibernateException(exception.getCause());
	}

	/**
	 * Gets the value as Map<String,Object> with the specified key from the
	 * datastore.
	 * 
	 * @param key
	 *            Used to retrieve the corresponding the value.
	 * @return Corresponding the value as Map.
	 */
	protected Map<String, Object> getEntityAsMap(EntityKey key) {
		Map<String, Object> map = this.getEntityWith(key);
		if (map == null) {
			return null;
		}
		for (Iterator<Entry<String, Object>> itr2 = map.entrySet().iterator(); itr2
				.hasNext();) {
			Entry en = itr2.next();
			try {
				if (en.getValue() == null) {
					en.setValue(null);
				} else if (en.getValue().getClass().isArray()) {
					en.setValue(this.toJSON(en.getValue()));
				} else if (this.jsonedDetector.isAssignable(en.getValue()
						.getClass())) {
					en.setValue(this.toJSON(en.getValue()));
				}
			} catch (Exception ex) {
				log.error(ex.getCause());
			}
		}

		return map;
	}

	/**
	 * Gets the declared fields from the specified class.
	 * 
	 * @param className
	 *            Class name used to get the declared fields.
	 * @return Field array storing the declared fields.
	 */
	protected Field[] getDeclaredFieldsFrom(String className) {
		try {
			return Class.forName(className).getDeclaredFields();
		} catch (SecurityException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (ClassNotFoundException e) {
			this.throwHibernateExceptionFrom(e);
		}

		return null;
	}

	/**
	 * Puts the JSONed value when the specified field type is one of JSONed
	 * class as the specified columnName on the specified Map.
	 * 
	 * @param field
	 *            Corresponding field to the columnName.
	 * @param columnName
	 *            Column name used on the datastore.
	 * @param map
	 *            Stores entity objects.
	 */
	protected void putJSONedValueTo(Field field, String columnName,
			Map<String, Object> map) {
		if (field.getType().isArray()) {
			map.put(columnName, this.fromJSON((String) map.get(columnName),
					field.getType()));
		} else if (this.classDetector.isWrapperClass(field.getType())) {
			map.put(columnName, this.classDetector.castWrapperClassFrom(
					map.get(columnName), field.getType()));
		} else if (this.jsonedDetector.isAssignable(field.getType())) {
			map.put(columnName, this.fromJSON((String) map.get(columnName),
					field.getType()));

		}
	}

	/**
	 * Creates JSON representation based on the specified object.
	 * 
	 * @param obj
	 *            To be JSONed.
	 * @return JSON representation of the specified object.
	 */
	protected String toJSON(Object obj) {
		return this.gson.toJson(obj);
	}

	/**
	 * Creates Object from the specified JSON representation based on the
	 * specified Class.
	 * 
	 * @param json
	 *            To be turned to Object.
	 * @param cls
	 *            Used to turn the JSON to object.
	 * @return Object representation of the JSON.
	 */
	protected Object fromJSON(String json, Class cls) {
		return this.gson.fromJson(json, cls);
	}

	/**
	 * Creates Map representation based on the specified JSON.
	 * 
	 * @param json
	 *            JSON string.
	 * @return Map representation of the JSON.
	 */
	protected Map<String, Object> getMapFromJSONString(String json) {

		Map<String, Object> map = new HashMap<String, Object>();
		try {
			JSONObject jsonObj = new JSONObject(json);
			for (Iterator<String> itr = jsonObj.keys(); itr.hasNext();) {
				String key = itr.next();
				log.info("key: " + key + " value: " + jsonObj.get(key)
						+ " type: "
						+ jsonObj.get(key).getClass().getCanonicalName());
				if (jsonObj.get(key) == JSONObject.NULL) {
					map.put(key, null);
				} else {
					map.put(key, jsonObj.get(key));
				}
			}
		} catch (JSONException e) {
			log.error(e.getCause());
			this.throwHibernateExceptionFrom(e);
		}

		log.info("returned map: " + map);
		return map;
	}

	protected Map<String, Object> toJSONIfNeeded(Map<String, Object> tuple) {
		if (tuple == null) {
			return null;
		}

		Map<String, Object> map = new HashMap<String, Object>();

		Set<Entry<String, Object>> entries = tuple.entrySet();
		for (Iterator<Entry<String, Object>> itr = entries.iterator(); itr
				.hasNext();) {
			Entry<String, Object> entry = itr.next();
			if (entry.getValue() != null
					&& String.class.isAssignableFrom(entry.getValue()
							.getClass())) {
				// entry.setValue(this.toJSON(entry.getValue()));
				map.put(entry.getKey(), this.toJSON(entry.getValue()));
			} else {
				map.put(entry.getKey(), entry.getValue());
			}
		}

		log.info("result tuple: " + tuple);
		return Collections.unmodifiableMap(map);
	}

	/**
	 * Creates a wrapper object using the specified primitive class and string
	 * value. This method calls a constructor with string parameter.
	 * 
	 * @param prmitiveClass
	 *            Class used to find the corresponding wrapper class.
	 * @param paramString
	 *            Set in the wrapper class constructor.
	 * @return Wrapper class object or null.
	 */
	protected Object createWrapperClassObjFrom(Class prmitiveClass,
			String paramString) {
		Class wrapperClass = ClassUtils.primitiveToWrapper(prmitiveClass);
		Constructor ctor;
		try {
			ctor = wrapperClass.getDeclaredConstructor(String.class);
			return ctor.newInstance(paramString);
		} catch (SecurityException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (NoSuchMethodException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IllegalArgumentException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (InstantiationException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IllegalAccessException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (InvocationTargetException e) {
			this.throwHibernateExceptionFrom(e);
		}

		return null;
	}
}
