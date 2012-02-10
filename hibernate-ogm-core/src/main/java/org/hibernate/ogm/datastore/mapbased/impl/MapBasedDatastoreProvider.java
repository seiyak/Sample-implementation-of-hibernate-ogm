package org.hibernate.ogm.datastore.mapbased.impl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.persistence.PessimisticLockException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.ogm.datastore.spi.AbstractMapBasedDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;
import org.jboss.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.JsonSyntaxException;

/**
 * This is an example a DatastoreProvider, implementing only the basic interface
 * needed by Hibernate OGM.
 * 
 * It does not support transactions, nor clustering nor it has monitoring or
 * capabilities to offload the contents to other storage. Most important, it
 * must be considered that different sessions won't be isolated unless they
 * avoid flushing.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class MapBasedDatastoreProvider extends
		AbstractMapBasedDatastoreProvider {

	// private static final Log log = LoggerFactory.make();
	private static final Logger log = Logger
			.getLogger(MapBasedDatastoreProvider.class);

	private final ConcurrentMap<EntityKey, Map<String, Object>> entitiesKeyValueStorage = new ConcurrentHashMap<EntityKey, Map<String, Object>>();
	private final ConcurrentMap<Object, ReadWriteLock> dataLocks = new ConcurrentHashMap();

	@Override
	public void stop() {
		entitiesKeyValueStorage.clear();
		log.info("Stopped and cleared MapBasedDatastoreProvider");
	}

	@Override
	public void start() {
		if (!this.checkRequiredSettings()) {
			throw new HibernateException(this.createStartUpErrorMessage());
		}
		log.info("MapBasedDatastoreProvider started");
	}

	@Override
	protected void putEntityToDatastore(EntityKey key, Map<String, Object> tuple) {
		entitiesKeyValueStorage.put(key, tuple);
	}

	@Override
	public Map<String, Object> getEntityTupleFromDatastore(EntityKey key) {

		Map<String, Object> tuple = this.getEntityAsMap(key);

		if (tuple == null) {
			return null;
		}

		this.createEntityTupleFromDatastore(key, tuple);

		log.info("return tuple: " + tuple);
		return tuple;
	}

	@Override
	protected Map<String, Object> getEntityWith(EntityKey key) {
		return entitiesKeyValueStorage.get(key);
	}

	@Override
	protected void removeEntityTupleFromDatastore(EntityKey key) {
		entitiesKeyValueStorage.remove(key);
	}

	/**
	 * Meant to execute assertions in tests only
	 * 
	 * @return a read-only view of the map containing the entities
	 */
	public Map<EntityKey, Map<String, Object>> getEntityMap() {

		for (Iterator<EntityKey> itr = entitiesKeyValueStorage.keySet()
				.iterator(); itr.hasNext();) {

			EntityKey key = itr.next();
			Map<String, Object> m = entitiesKeyValueStorage.get(key);
			for (Iterator<Entry<String, Object>> itr2 = m.entrySet().iterator(); itr2
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

		}
		return Collections.unmodifiableMap(entitiesKeyValueStorage);
	}

	/**
	 * Checks if the required properties on hibernate.properties are set
	 * correctly.
	 * 
	 * @return True if they are set correctly, false otherwise.
	 */
	@Override
	protected boolean checkRequiredSettings() {

		if (this.getRequiredPropertyValues().get("provider")
				.equals(this.getClass().getCanonicalName())
				&& this.getRequiredPropertyValues().get("grid_dialect")
						.equals(this.getDefaultDialect().getCanonicalName())
				&& this.getRequiredPropertyValues().get("dialect")
						.equals("org.hibernate.ogm.dialect.NoopDialect")) {
			return true;
		}

		return false;
	}

	@Override
	protected void connectToServerIfNeeded() {
		log.info("not called by this datastore : "
				+ this.getClass().getCanonicalName());
	}
}
