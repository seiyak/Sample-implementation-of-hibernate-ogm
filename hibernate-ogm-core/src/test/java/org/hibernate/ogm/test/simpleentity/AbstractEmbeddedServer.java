package org.hibernate.ogm.test.simpleentity;

import java.io.File;

import org.hibernate.cfg.Environment;

public abstract class AbstractEmbeddedServer implements EmbeddedServerAware {

	protected static final String DEBUG_LOCATION = Environment.getProperties()
			.getProperty("hibernate.ogm.datastore.provider_debug_location");

	// Deletes all files and subdirectories under dir.
	// Returns true if all deletions were successful.
	// If a deletion fails, the method stops attempting to delete and returns
	// false.
	public boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return dir.delete();
	}

	public abstract boolean removeAllEntries();
}
