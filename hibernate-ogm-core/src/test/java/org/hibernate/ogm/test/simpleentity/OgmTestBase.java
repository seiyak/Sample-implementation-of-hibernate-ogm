package org.hibernate.ogm.test.simpleentity;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.hibernate.cfg.Environment;

import junit.framework.TestCase;

public abstract class OgmTestBase extends TestCase {

	private EmbeddedServerAware embeddedServer;

	protected void startEmbeddedServer() {
		if (this.embeddedServer == null) {
			throw new RuntimeException(
					"OGMTestBase.embeddedServer is not set correctly. Please set it before running test cases");
		}
		this.embeddedServer.start();
	}

	protected void stopEmbeddedServer() {
		this.embeddedServer.stop();
		this.embeddedServer = null;
	}

	protected void setUpEmbeddedServer() {
		String provider = Environment.getProperties().getProperty(
				"hibernate.ogm.datastore.provider");
		if (provider
				.equals("org.hibernate.ogm.datastore.mapbased.impl.VoldemortDatastoreProvider")) {
			if (this.embeddedServer != null) {
				this.embeddedServer.stop();
				this.embeddedServer = null;
			}
			this.embeddedServer = new EmbeddedVoldemort();
		} else if (provider
				.equals("org.hibernate.ogm.datastore.mapbased.impl.RedisDatastoreProvider")) {
			this.embeddedServer = new RedisServer();
		} else if (provider
				.equals("org.hibernate.ogm.datastore.mapbased.impl.RiakDatastoreProvider")) {
			this.embeddedServer = new RiakServer();
		} else {
			this.embeddedServer = new EmbeddedNoop();
		}

		this.startEmbeddedServer();
	}

	public String fullTestName() {
		return this.getClass().getName() + "#" + this.getName();
	}

	protected Method findTestMethod() {
		String fName = getName();
		assertNotNull(fName);
		Method runMethod = null;
		try {
			runMethod = getClass().getMethod(fName);
		} catch (NoSuchMethodException e) {
			fail("Method \"" + fName + "\" not found");
		}
		if (!Modifier.isPublic(runMethod.getModifiers())) {
			fail("Method \"" + fName + "\" should be public");
		}
		return runMethod;
	}
}
