package org.hibernate.ogm.dialect;

import org.hibernate.dialect.Dialect;

import voldemort.client.StoreClient;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class VoldemortDialect extends Dialect {

	public Versioned get(StoreClient client, Object key) {
		return client.get(key);
	}

	public Object getValue(StoreClient client, Object key) {
		return client.getValue(key);
	}

	public boolean delete(StoreClient client, Object key) {
		return client.delete(key);
	}

	public Version put(StoreClient client, Object key, Object value) {
		return client.put(key, value);
	}
}
