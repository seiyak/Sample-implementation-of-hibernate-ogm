package org.hibernate.ogm.datastore.spi;

import org.apache.log4j.Logger;

public class JSONedClassDetector {

	private static enum JSONedClasses {
		JAVA_UTIL_CALENDAR("java.util.Calendar"), JAVA_UTIL_DATE(
				"java.util.Date");

		private static final Logger log = Logger
				.getLogger(JSONedClassDetector.class);
		private Class cls;

		JSONedClasses(String className) {
			this.cls = this.getClassWith(className);
		}

		/**
		 * Gets the class representation with the specified class name.
		 * 
		 * @param className
		 *            Used to create Class.
		 * @return Class object based on the specified class name.
		 */
		private Class getClassWith(String className) {
			try {
				return Class.forName(className);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		/**
		 * Checks if this class is assignable from the specified class or not.
		 * 
		 * @param cls
		 *            Used to examine the assignability.
		 * @return True if it's assignable, false otherwise.
		 */
		public static boolean canBeAssignableFrom(Class cls) {
			for (JSONedClasses jsonedClass : values()) {
				if (jsonedClass.getEachClass().isAssignableFrom(cls)) {
					return true;
				}
			}

			return false;
		}

		public Class getEachClass() {
			return this.cls;
		}
	}

	public boolean isAssignable(Class cls) {
		return JSONedClasses.canBeAssignableFrom(cls);
	}
}
