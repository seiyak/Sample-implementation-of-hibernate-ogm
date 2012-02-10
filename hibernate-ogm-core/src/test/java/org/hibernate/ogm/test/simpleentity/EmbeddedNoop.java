package org.hibernate.ogm.test.simpleentity;

import org.apache.log4j.Logger;

public class EmbeddedNoop extends AbstractEmbeddedServer {

	private static final Logger log = Logger.getLogger(EmbeddedNoop.class);

	@Override
	public void start() {
		log.info("starting EmbeddedNoop ...");

	}

	@Override
	public void stop() {
		log.info("stopping EmbeddedNoop ...");

	}

	@Override
	public boolean removeAllEntries() {
		log.info("removing all entries");
		return false;
	}

}
