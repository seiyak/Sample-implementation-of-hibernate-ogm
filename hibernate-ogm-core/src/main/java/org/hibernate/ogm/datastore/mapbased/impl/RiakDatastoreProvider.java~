package org.hibernate.ogm.datastore.mapbased.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.ogm.datastore.spi.AbstractMapBasedDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.persister.EntityKeyBuilder;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.WriteBucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.convert.reflect.ClassUtil;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.StoreObject;

/**
 * 
 * @author seiyak <skawashima@uchicago.edu>
 * 
 */
public class RiakDatastoreProvider extends AbstractMapBasedDatastoreProvider {

	private static final Logger log = Logger
			.getLogger(RiakDatastoreProvider.class);
	private IRiakClient riak;
	//// Now I believe that this bucket property is not thread safe. I need to search more
	//// and change it if necessary shortly. Probably every time I would need something like,
	//// StoreObject storeObj = this.riak.store(...);
	//// this.setUpPropertyOn(storeObj, this.storeConfig);
	//// storeObj.execute();
	//// 
	private Bucket bucket;
	private WriteBucket bootStrapBucket;
	private final Set<Entry<Object, Object>> fetchConfig = new HashSet<Entry<Object, Object>>();
	private final Set<Entry<Object, Object>> storeConfig = new HashSet<Entry<Object, Object>>();
	private final Set<Entry<Object, Object>> deleteConfig = new HashSet<Entry<Object, Object>>();
	// // for testing purpose
	private final ConcurrentHashMap<String, Map<String, Object>> idsTable = new ConcurrentHashMap<String, Map<String, Object>>();

	private static enum KeyWords {

		RIAK_BUCKET_CONFIG_PREFIX(
				"hibernate.ogm.datastore.provider.riak_config.bucket"), RIAK_CLIENT_CONFIG_PREFIX(
				"hibernate.ogm.datastore.provider.riak_config.client"), RIAK_BUCKET_NAME_CONFIG(
				"hibernate.ogm.datastore.provider.riak_config.client.bucket_name"), RIAK_DELETE_CONFIG_PREFIX(
				"hibernate.ogm.datastore.provider.riak_config.delete"), RIAK_FETCH_CONFIG_PREFIX(
				"hibernate.ogm.datastore.provider.riak_config.fetch"), RIAK_STORE_CONFIG_PREFIX(
				"hibernate.ogm.datastore.provider.riak_config.store"), HTTP_CLIENT(
				"http"), PB_CLIENT("pb"), RIAK_DEFAULT_BUCKET_NAME("riakBucket");

		private final String keyWord;

		KeyWords(String keyWord) {
			this.keyWord = keyWord;
		}

		public String getKeyWord() {
			return this.keyWord;
		}
	}

	private static enum Quorums {

		DW("dw"), PR("pr"), PW("pw"), R("r"), RW("rw"), W("w");

		private final String quorum;

		Quorums(String quorum) {
			this.quorum = quorum;
		}

		public String getQuorum() {
			return this.quorum;
		}

		public static boolean isQuorom(String quorum) {
			for (Quorums q : values()) {
				if (q.getQuorum().equals(quorum)) {
					return true;
				}
			}

			return false;
		}
	}

	@Override
	public void start() {
		log.info("starting Riak");
		// try {
		this.setUpRiakClient();
		// } catch (Exception ex) {
		// log.warn("startup fails. about to call stop(). reason is below.");
		// log.error(ex.getCause());
		// this.stop();
		// }
	}

	/**
	 * Sets up Riak client.
	 */
	private void setUpRiakClient() {

		List<Entry<Object, Object>> bucketProps = new ArrayList<Entry<Object, Object>>();
		boolean called = false;

		Set<Entry<Object, Object>> props = Environment.getProperties()
				.entrySet();
		for (Iterator<Entry<Object, Object>> itr = props.iterator(); itr
				.hasNext();) {

			Entry<Object, Object> prop = itr.next();
			String key = (String) prop.getKey();

			if (key.equals(KeyWords.RIAK_CLIENT_CONFIG_PREFIX.getKeyWord())) {
				this.callRiakFactoryMethod((String) prop.getValue());
				called = true;
			} else if (key.startsWith(KeyWords.RIAK_BUCKET_CONFIG_PREFIX
					.getKeyWord())) {
				if (called) {
					this.setUpPropertyOn(this.bootStrapBucket, prop);
				} else {
					bucketProps.add(prop);
				}
			} else if (key.startsWith(KeyWords.RIAK_DELETE_CONFIG_PREFIX
					.getKeyWord())) {
				this.deleteConfig.add(prop);
			} else if (key.startsWith(KeyWords.RIAK_FETCH_CONFIG_PREFIX
					.getKeyWord())) {
				this.fetchConfig.add(prop);
			} else if (key.startsWith(KeyWords.RIAK_STORE_CONFIG_PREFIX
					.getKeyWord())) {
				this.storeConfig.add(prop);
			}
		}

		if (!bucketProps.isEmpty()) {
			this.setUpPropertyOn(this.bootStrapBucket, bucketProps);
		}

		try {
			this.bucket = this.bootStrapBucket.execute();
			this.bootStrapBucket = null;
		} catch (RiakRetryFailedException e) {
			this.throwHibernateExceptionFrom(e);
		}

		// this.deleteConfig = Collections.unmodifiableSet(this.deleteConfig);
		// this.storeConfig = Collections.unmodifiableSet(this.storeConfig);
		// this.fetchConfig = Collections.unmodifiableSet(this.fetchConfig);

		// log.info("delete config size: " + this.deleteConfig.size());
		// log.info("store config size: " + this.storeConfig.size());
		// log.info("fetch config size: " + this.fetchConfig.size());
		log.info("bucket is ready to use");
	}

	/**
	 * Calls RiakFactory.pbClient() or RiakFactory.httpClient() method depending
	 * on the specified client type. This method expects the specified client
	 * type is either "pb" or "http". If the client type is null, uses "http" as
	 * the default.
	 * 
	 * @param clientType
	 *            Client type for the Riak client.
	 */
	private void callRiakFactoryMethod(String clientType) {
		this.checkRiakClientType(clientType);
	}

	/**
	 * Checks if the specified client type is valid or not. The valid client
	 * type is "pb","http" or null.
	 * 
	 * @param clientType
	 *            Client type for the Riak client.
	 */
	private void checkRiakClientType(String clientType) {

		boolean done = false;
		if (clientType == null
				|| clientType.equals(KeyWords.HTTP_CLIENT.getKeyWord())) {
			this.createRiakHttpClient();
			done = true;
		} else if (clientType.equals(KeyWords.PB_CLIENT.getKeyWord())) {
			this.createRiakPbClient();
			done = true;
		}

		if (done) {
			this.createBucket();
			return;
		}

		throw new HibernateException(
				"please specify one of 'pb','http' or 'null' as the Riak client type,"
						+ clientType);
	}

	/**
	 * Creates Riak http client calling RiakFactory.httpClient().
	 */
	private void createRiakHttpClient() {
		try {
			log.info("about to create Riak http client");
			this.riak = RiakFactory.httpClient();
		} catch (RiakException e) {
			throw new HibernateException(e);
		}
	}

	/**
	 * Create Riak pb client calling RiakFactory.pbClient().
	 */
	private void createRiakPbClient() {

		try {
			log.info("about to create Riak pb client");
			this.riak = RiakFactory.pbcClient();
		} catch (RiakException e) {
			throw new HibernateException(e);
		}
	}

	/**
	 * Creates a bucket using the specified bucket name in hibernate.properties.
	 * If the bucket name whose property key is
	 * hibernate.ogm.datastore.provider.riak_config.client.bucket_name is not
	 * set, then 'riakBucket' as the bucket name is set by default.
	 */
	private void createBucket() {
		if (this.bootStrapBucket != null) {
			return;
		}

		this.checkRiakClientAndThrow();

		String bucketName = Environment.getProperties().getProperty(
				KeyWords.RIAK_BUCKET_NAME_CONFIG.getKeyWord());
		bucketName = bucketName == null ? KeyWords.RIAK_DEFAULT_BUCKET_NAME
				.getKeyWord() : bucketName;
		log.info("bucket name: " + bucketName);
		this.bootStrapBucket = this.riak.createBucket(bucketName);
	}

	/**
	 * Sets specified property on the specified object. This method expects the
	 * specified prop is instance of Collection or Entry<Object,Object>.
	 * 
	 * @param obj
	 *            Object where the set up is taken place.
	 * @param prop
	 *            Property to be set on the specified object.
	 */
	private void setUpPropertyOn(Object obj, Object prop) {
		this.checkRiakClientAndThrow();
		if (prop instanceof Collection) {
			this.setUpPropOn(obj, (Collection<Entry<Object, Object>>) prop);
		} else if (prop instanceof Entry) {
			this.setUpPropOn(obj, (Entry<Object, Object>) prop);
		} else {
			throw new HibernateException(
					"bucketProp is neither Collection nor Entry but, "
							+ prop.getClass().getCanonicalName());
		}
	}

	/**
	 * Checks if Riak client is not set or not. If it's not, throws
	 * HibernateException. If it is,does nothing.
	 */
	private void checkRiakClientAndThrow() {
		if (this.riak == null) {
			throw new HibernateException(
					"for some reason, riak client has not been initialized. please do so first.");
		}
	}

	/**
	 * Sets the specified key-value pair as the corresponding property on the
	 * specified object.
	 * 
	 * @param obj
	 *            Object where the setup is taken place.
	 * @param prop
	 *            Key-value pair to be set.
	 */
	private void setUpPropOn(Object obj, Entry<Object, Object> prop) {

		String key = (String) prop.getKey();
		// log.info("key: " + key + " quorum: "
		// + key.substring(key.lastIndexOf("_") + 1));
		String specificPropName = key.substring(key.lastIndexOf("_") + 1);
		if (Quorums.isQuorom(specificPropName)) {
			this.setUpQuorum(obj, specificPropName, (String) prop.getValue());
		} else {
			this.setUpOtherPropsOn(obj, specificPropName,
					(String) prop.getValue());
		}
	}

	/**
	 * Sets other properties than quorum properties.
	 * 
	 * @param obj
	 *            Object where the setup is taken place.
	 * @param specificPropName
	 *            Property name to be set.
	 * @param value
	 *            Value to be set.
	 */
	private void setUpOtherPropsOn(Object obj, String specificPropName,
			String value) {
		// // this method is error-prone in general,
		//// but it's ok to do this in this specific context.
		Method method = this.getMethodWith(obj.getClass(), specificPropName);
		if (method == null) {
			throw new HibernateException(
					"could not find the specified method name, "
							+ specificPropName + " on "
							+ obj.getClass().getCanonicalName());
		}

		// log.info("found corresponding method: " + method.getName());
		if (method.getParameterTypes()[0].isPrimitive()) {
			// log.info("parameter is primitive");
			this.setUpPrimitivePropOn(obj, method, value,
					method.getParameterTypes()[0]);
		} else {
			// log.info("parameter is object");
			this.setUpObjectPropOn(obj, method, value);
		}
	}

	/**
	 * Gets a method searching for the specified name on the specified class.
	 * 
	 * @param cls
	 *            Class where the method search is taken place.
	 * @param methodName
	 *            Method name to be looked up.
	 * @return Specified method object or null when the specified method doesn't
	 *         exist.
	 */
	protected Method getMethodWith(Class cls, String methodName) {
		Method[] methods = cls.getDeclaredMethods();
		if (methods == null) {
			return null;
		}

		for (Method method : methods) {
			if (method.getName().equals(methodName)) {
				return method;
			}
		}

		return null;
	}

	/**
	 * Sets primitive value calling the corresponding method on the specified
	 * object.
	 * 
	 * @param obj
	 *            Object where the setup is taken place.
	 * @param method
	 *            Corresponding method to set the specified value on the object.
	 * @param value
	 *            Value to be set.
	 * @param primitiveClass
	 *            Primitive class to be set.
	 */
	private void setUpPrimitivePropOn(Object obj, Method method, String value,
			Class primitiveClass) {
		Object targetObj = this
				.createWrapperClassObjFrom(primitiveClass, value);
		try {
			// log.info("about to set up primitive prop: " + method.getName()
			// + " value: " + value);
			method.invoke(
					obj,
					targetObj
							.getClass()
							.getDeclaredMethod(
									primitiveClass.getCanonicalName() + "Value")
							.invoke(targetObj));
		} catch (IllegalArgumentException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (SecurityException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IllegalAccessException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (InvocationTargetException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (NoSuchMethodException e) {
			this.throwHibernateExceptionFrom(e);
		}
	}

	/**
	 * Sets object value calling the corresponding method on the specified
	 * object.
	 * 
	 * @param obj
	 *            Object where the setup is taken place.
	 * @param method
	 *            Corresponding method to set the specified value on the object.
	 * @param value
	 *            Value to be set.
	 */
	private void setUpObjectPropOn(Object obj, Method method, String value) {
		// //TODO test this execution path
		try {
			method.invoke(obj, Class.forName(value).newInstance());
		} catch (IllegalArgumentException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IllegalAccessException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (InvocationTargetException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (InstantiationException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (ClassNotFoundException e) {
			this.throwHibernateExceptionFrom(e);
		}
	}

	/**
	 * Sets specified properties on the specified object.
	 * 
	 * @param obj
	 *            Object where the setup is taken place.
	 * @param props
	 *            Properties to be set.
	 */
	private void setUpPropOn(Object obj, Collection<Entry<Object, Object>> props) {
		for (Iterator<Entry<Object, Object>> itr = props.iterator(); itr
				.hasNext();) {
			Entry<Object, Object> entry = itr.next();
			this.setUpPropOn(obj, entry);
		}
	}

	/**
	 * Sets quorum property with the specified value on the specified object.
	 * 
	 * @param obj
	 *            Object where the setting is taken place.
	 * @param quorum
	 *            Quorum name to be set.
	 * @param value
	 *            Quorum value to be set.
	 */
	private void setUpQuorum(Object obj, String quorum, String value) {

		try {
			Object targetObj = this.createWrapperClassObjFrom(int.class, value);
			// log.info("about to set quorum with: " + quorum + " " + value);
			obj.getClass()
					.getDeclaredMethod(quorum, int.class)
					.invoke(obj,
							targetObj.getClass().getDeclaredMethod("intValue")
									.invoke(targetObj));
		} catch (IllegalArgumentException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (SecurityException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IllegalAccessException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (InvocationTargetException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (NoSuchMethodException e) {
			this.throwHibernateExceptionFrom(e);
		}
	}

	@Override
	public void stop() {
		log.info("stopping Riak");
		this.deleteConfig.clear();
		this.storeConfig.clear();
		this.fetchConfig.clear();
		this.idsTable.clear();
	}

	@Override
	protected void putEntityToDatastore(EntityKey key, Map<String, Object> tuple) {
		this.connectToServerIfNeeded();

		// log.info("about to set store props");
		StoreObject<Map<String, Object>> storeObj = this.bucket.store(
				this.toJSON(key.getEntityKeyAsMap()), tuple);
		this.setUpPropertyOn(storeObj, this.storeConfig);
		// log.info("about to store key: " +
		// this.toJSON(key.getEntityKeyAsMap())
		// + " value: " + tuple);
		try {
			storeObj.execute();
			this.idsTable.put(this.toJSON(key.getEntityKeyAsMap()), tuple);
		} catch (RiakRetryFailedException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (UnresolvedConflictException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (ConversionException e) {
			this.throwHibernateExceptionFrom(e);
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
		// log.info("about to set delete prop");
		DeleteObject deleteObj = this.bucket.delete(this.toJSON(key
				.getEntityKeyAsMap()));
		this.setUpPropertyOn(deleteObj, this.deleteConfig);
		log.info("about to delete key: " + this.toJSON(key.getEntityKeyAsMap()));
		try {
			deleteObj.execute();
			this.idsTable.remove(this.toJSON(key.getEntityKeyAsMap()));
		} catch (RiakException e) {
			this.throwHibernateExceptionFrom(e);
		}
	}

	@Override
	protected Map<String, Object> getEntityWith(EntityKey key) {

		this.connectToServerIfNeeded();
		// log.info("about to set fetch prop");
		FetchObject<IRiakObject> fetchObj = this.bucket.fetch(this.toJSON(key
				.getEntityKeyAsMap()));
		this.setUpPropOn(fetchObj, this.fetchConfig);
		try {
			// log.info("about to fetch key: "
			// + this.toJSON(key.getEntityKeyAsMap()));
			IRiakObject obj = fetchObj.execute();
			if (obj != null) {
				log.info("fetched object as string: " + obj.getValueAsString());
			} else {
				log.info("fetched object is null");
			}
			if (obj != null) {
				return this.getMapFromJSONString(obj.getValueAsString());
			}
		} catch (UnresolvedConflictException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (RiakRetryFailedException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (ConversionException e) {
			this.throwHibernateExceptionFrom(e);
		}

		return null;
	}

	@Override
	protected boolean checkRequiredSettings() {
		if (this.getRequiredPropertyValues().get("provider")
				.equals(this.getClass().getCanonicalName())
				&& this.getRequiredPropertyValues().get("grid_dialect")
						.equals(this.getDefaultDialect().getCanonicalName())
				&& this.getRequiredPropertyValues().get("dialect")
						.equals("org.hibernate.ogm.dialect.RiakDialect")) {
			return true;
		}

		return false;
	}

	@Override
	protected void connectToServerIfNeeded() {
		this.setUpRiakClient();
	}

	@Override
	public Map<EntityKey, Map<String, Object>> getEntityMap() {
		Map<EntityKey, Map<String, Object>> map = new HashMap<EntityKey, Map<String, Object>>();
		Iterator<String> itr = null;

		// // this.bucket.keys().iterator() call has some random issue.
		itr = this.idsTable.keySet().iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			log.info("key in getEntityMap(): " + key);
			try {
				JSONObject jsonKey = new JSONObject(key);
				EntityKey entityKey = new EntityKey(
						(String) jsonKey.get("table"),
						(String) jsonKey.get("id"),
						EntityKeyBuilder.DEBUG_OGM_PERSISTER.getEntityName(),
						EntityKeyBuilder
								.getColumnMap(EntityKeyBuilder.DEBUG_OGM_PERSISTER));
				map.put(entityKey, this.getEntityTuple(entityKey));
			} catch (JSONException e) {
				this.throwHibernateExceptionFrom(e);
			}
		}

		return map;
	}
}
