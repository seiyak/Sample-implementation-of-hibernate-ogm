package org.hibernate.ogm.datastore.mapbased.impl;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.ogm.datastore.spi.AbstractMapBasedDatastoreProvider;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.persister.EntityKeyBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

/**
 * 
 * @author seiyak <skawashima@uchicago.edu>
 * 
 */
public class VoldemortDatastoreProvider extends
		AbstractMapBasedDatastoreProvider {

	private static final Logger log = Logger
			.getLogger(VoldemortDatastoreProvider.class);

	private StoreClientFactory clientFactory;
	private StoreClient client;
	private final ObjectMapper mapper = new ObjectMapper();;
	private final ConcurrentMap<String, Set<Serializable>> tableIds = new ConcurrentHashMap<String, Set<Serializable>>();

	@Override
	public void start() {
		log.info("starting Voldemort lazily");
	}

	@Override
	public void stop() {
		log.info("stopping Voldemort");
		this.tableIds.clear();
		this.clientFactory.close();
	}

	@Override
	protected void putEntityToDatastore(EntityKey key, Map<String, Object> tuple) {
		this.connectToServerIfNeeded();
		log.info("putting to voldemort key: " + key + " value: "
				+ tuple.toString());
		try {
			this.addEntryToIdTable(key);
			this.client.put(key.getEntityKeyAsMap(),
					this.mapper.writeValueAsBytes(tuple));
		} catch (JsonGenerationException e) {
			this.checkAndRemoveEntry(key);
			this.throwHibernateExceptionFrom(e);
		} catch (JsonMappingException e) {
			this.checkAndRemoveEntry(key);
			this.throwHibernateExceptionFrom(e);
		} catch (IOException e) {
			this.checkAndRemoveEntry(key);
			this.throwHibernateExceptionFrom(e);
		}
	}

	/**
	 * Checks if the key is already stored and if so, removes it. This method is
	 * called by putEntity() method when any exceptions happen.
	 * 
	 * @param key
	 *            Key to be used to remove the corresponding value.
	 */
	private void checkAndRemoveEntry(EntityKey key) {
		if (this.tableIds.containsKey(key)) {
			this.tableIds.remove(key);
		}
	}

	@Override
	protected Map<String, Object> getEntityWith(EntityKey key) {
		this.connectToServerIfNeeded();
		log.info("getting with key: " + key + " entity name: "
				+ key.getEntityName());
		Versioned v = this.client.get(key.getEntityKeyAsMap());
		if (v == null) {
			return null;
		}

		Map<String, Object> tuple = null;
		try {
			tuple = this.mapper.readValue((byte[]) v.getValue(), 0,
					((byte[]) v.getValue()).length, Map.class);
			return tuple;
		} catch (JsonParseException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (JsonMappingException e) {
			this.throwHibernateExceptionFrom(e);
		} catch (IOException e) {
			this.throwHibernateExceptionFrom(e);
		}

		return null;
	}

	@Override
	public Map<String, Object> getEntityTupleFromDatastore(EntityKey key) {

		Map<String, Object> tuple = this.getEntityAsMap(key);
		this.createEntityTupleFromDatastore(key, tuple);
		log.info("return tuple: " + tuple);
		return tuple;
	}

	/**
	 * Stores the specified key to this object.
	 * 
	 * @param key
	 *            Entity key to be stored.
	 */
	private void addEntryToIdTable(EntityKey key) {

		Serializable rtnId = null;
		if (this.tableIds.get(key.getTableName()) != null) {
			log.info("about to add. current table id size: "
					+ this.tableIds.get(key.getTableName()).size());
		}

		if (this.tableIds.get(key.getTableName()) == null) {
			Set<Serializable> set = new HashSet<Serializable>();
			set.add(key.getId());
			rtnId = (Serializable) this.tableIds.put(key.getTableName(), set);
			log.info("previous value: " + rtnId);
		} else {
			boolean done = this.tableIds.get(key.getTableName()).add(
					key.getId());
			if (done) {
				log.info("sccessfully added id with: " + key.getId()
						+ " size: "
						+ this.tableIds.get(key.getTableName()).size());
			} else {
				log.warn("the id is already stored: " + key + " size: "
						+ this.tableIds.get(key.getTableName()).size());
			}
		}

		log.info("added key with: " + key.getId() + " current table id size: "
				+ this.tableIds.get(key.getTableName()).size());
		this.showAllTableIds();
	}

	@Override
	protected void removeEntityTupleFromDatastore(EntityKey key) {
		this.connectToServerIfNeeded();
		log.info("removing from voldemort with key: " + key);
		this.removeEntryFromIdTable(key);
		boolean rtn = this.client.delete(key.getEntityKeyAsMap());
		if (rtn) {
			log.info("successfully deleted the object with key: " + key);
		} else {
			log.warn("could not delete the object with key: " + key);
		}

		this.showAllTableIds();
	}

	/**
	 * Removes the specified key from this object.
	 * 
	 * @param key
	 *            Entity key to be removed.
	 */
	private void removeEntryFromIdTable(EntityKey key) {
		log.info("about to delete. current id size: "
				+ this.tableIds.get(key.getTableName()).size());
		boolean rtn = this.tableIds.get(key.getTableName()).remove(key.getId());
		if (rtn) {
			log.info("successfully deleted the id. size: "
					+ this.tableIds.get(key.getTableName()).size());
		} else {
			log.warn("could not delete the id. size: "
					+ this.tableIds.get(key.getTableName()).size());
		}
	}

	/**
	 * Shows all the table name and id pairs currently stored on this object.
	 */
	private void showAllTableIds() {
		StringBuilder stringBuilder = new StringBuilder();
		Set<Entry<String, Set<Serializable>>> entries = this.tableIds
				.entrySet();
		boolean found = false;
		for (Iterator<Entry<String, Set<Serializable>>> itr = entries
				.iterator(); itr.hasNext();) {
			Entry<String, Set<Serializable>> entry = itr.next();
			this.generateAllTableIdsMessage(entry, stringBuilder);
			found = true;
		}

		if (found) {
			log.info(stringBuilder);
		} else {
			log.info("currently there are no ids stored");
		}
	}

	/**
	 * Generates a message for showing all the table name and id pairs.
	 * 
	 * @param entry
	 *            Stores table name and the corresponding ids.
	 * @param stringBuilder
	 *            Used to build the message.
	 */
	private void generateAllTableIdsMessage(
			Entry<String, Set<Serializable>> entry, StringBuilder stringBuilder) {

		stringBuilder.append("table name: " + entry.getKey() + "\n");
		if (entry.getValue().isEmpty()) {
			stringBuilder.append("\tall the ids on table, " + entry.getKey()
					+ " are already deleted.\n");
		} else {
			for (Iterator<Serializable> itr = entry.getValue().iterator(); itr
					.hasNext();) {
				stringBuilder.append("\tid: " + itr.next() + "\n");
			}
		}
	}

	/**
	 * Meant to execute assertions in tests only. Delete
	 * EntityKeyBuilder.DEBUG_OGM_PERSISTER when tests are done.
	 * 
	 * @return a read-only view of the map containing the entities
	 */
	public Map<EntityKey, Map<String, Object>> getEntityMap() {
		this.connectToServerIfNeeded();

		Map<EntityKey, Map<String, Object>> map = new HashMap<EntityKey, Map<String, Object>>();
		Set<Entry<String, Set<Serializable>>> entries = this.tableIds
				.entrySet();
		for (Iterator<Entry<String, Set<Serializable>>> itr = entries
				.iterator(); itr.hasNext();) {
			Entry<String, Set<Serializable>> entry = itr.next();
			for (Iterator<Serializable> itr2 = entry.getValue().iterator(); itr2
					.hasNext();) {
				Serializable id = itr2.next();
				EntityKey entityKey = new EntityKey(
						entry.getKey(),
						id,
						EntityKeyBuilder.DEBUG_OGM_PERSISTER.getEntityName(),
						EntityKeyBuilder
								.getColumnMap(EntityKeyBuilder.DEBUG_OGM_PERSISTER));
				map.put(entityKey, this.getEntityTuple(entityKey));
			}
		}

		return map;
	}

	/**
	 * Connects to Voldemort server lazily when it's required.
	 */
	@Override
	protected void connectToServerIfNeeded() {
		if (!this.checkRequiredSettings()) {
			throw new HibernateException(this.createStartUpErrorMessage());
		}

		if (this.clientFactory == null && this.client == null) {
			this.clientFactory = new SocketStoreClientFactory(
					new ClientConfig().setBootstrapUrls(this
							.getRequiredPropertyValues().get("provider_url")));
			this.client = this.clientFactory.getStoreClient(this
					.getVoldemortSpecificSettings());
		}
	}

	/**
	 * Checks if the Voldemort specific settings are set correctly. Currently
	 * there is only one setting for the store name.
	 * 
	 * @return True if the option is set, false otherwise.
	 */
	private boolean checkVoldemortSpecificSettings() {
		if (this.getVoldemortSpecificSettings().equals("")) {
			return false;
		}

		return true;
	}

	/**
	 * Gets the Voldemort specific setting values. Currently there is only one
	 * settings for the store name.
	 * 
	 * @return Store name if the option is set, empty string otherwise.
	 */
	private String getVoldemortSpecificSettings() {

		String storeName = Environment.getProperties().getProperty(
				"hibernate.ogm.datastore.voldemort_store");
		return storeName == null ? "" : storeName;
	}

	@Override
	protected boolean checkRequiredSettings() {
		if (this.getRequiredPropertyValues().get("provider")
				.equals(this.getClass().getCanonicalName())
				&& this.getRequiredPropertyValues().get("grid_dialect")
						.equals(this.getDefaultDialect().getCanonicalName())
				&& this.getRequiredPropertyValues().get("dialect")
						.equals("org.hibernate.ogm.dialect.VoldemortDialect")
				&& this.getRequiredPropertyValues().get("provider_url") != null
				&& this.checkVoldemortSpecificSettings()) {
			return true;
		}
		return false;
	}
}
