package org.hibernate.ogm.test.simpleentity;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;

public class RiakServer extends AbstractEmbeddedServer {

	private static final Logger log = Logger.getLogger(RiakServer.class);
	private IRiakClient riak;
	private Bucket bucket;

	@Override
	public void start() {
		log.info("starting riak from test");
		this.callRiakFactoryMethod((String) Environment.getProperties()
				.getProperty(
						"hibernate.ogm.datastore.provider.riak_config.client"));
	}

	@Override
	public void stop() {
		log.info("stopping riak from test");
		log.info("remove all the entries, " + this.removeAllEntries());
	}

	@Override
	public boolean removeAllEntries() {
		if (this.riak == null || this.bucket == null) {
			throw new HibernateException(
					"for some reason, riak and/or bucket are null");
		}

		try {
			Iterator<String> itr = this.bucket.keys().iterator();
			while (itr.hasNext()) {
				String key = itr.next();
				this.bucket.delete(key).execute();
			}
			return true;
		} catch (RiakException e) {
			throw new HibernateException(e);
		}
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
		if (clientType == null || clientType.equals("http")) {
			this.createRiakHttpClient();
			done = true;
		} else if (clientType.equals("pb")) {
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

	private void createBucket() {
		if (this.bucket == null) {
			try {
				this.bucket = this.riak
						.createBucket(
								(String) Environment
										.getProperties()
										.getProperty(
												"hibernate.ogm.datastore.provider.riak_config.client.bucket_name"))
						.execute();
			} catch (RiakRetryFailedException e) {
				throw new HibernateException(e);
			}
		}
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

}
