package org.hibernate.ogm.datastore.mapbased.impl;

import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;

public class WrapperClassDetector {

	private static final Logger log = Logger
			.getLogger(WrapperClassDetector.class);

	private static enum WrapperClasses {
		JAVA_LANG_BYTE("java.lang.Byte"), JAVA_LANG_SHORT("java.lang.Short"), JAVA_LANG_INTEGER(
				"java.lang.Integer"), JAVA_LANG_LONG("java.lang.Long"), JAVA_LANG_FLOAT(
				"java.lang.Float"), JAVA_LANG_DOUBLE("java.lang.Double"), JAVA_LANG_CHARACTER(
				"java.lang.Character"), JAVA_LANG_BOOLEAN("java.lang.Boolean");

		String className;

		private WrapperClasses(String className) {
			this.className = className;
		}

		/**
		 * Gets the class name of this enum.
		 * @return
		 */
		public String getClassName() {
			return this.className;
		}
		
		/**
		 * Checks if the specified class name is one of the wrapper class names or not.
		 * @param className Class name to be examined.
		 * @return True if the specified class name is one of the wrapper class, false otherwise.
		 */
		public static boolean isWrapper(String className) {

			for (WrapperClasses clss : values()) {
				if (clss.getClassName().equals(className)) {
					return true;
				}
			}

			return false;
		}
	}

	/**
	 * Checks if the specified class is a wrapper class for the corresponding
	 * primitive type.
	 * 
	 * @param cls
	 *            Class to be examined.
	 * @return True if it's a wrapper class, false otherwise.
	 */
	public boolean isWrapperClass(Class cls) {
		boolean b = WrapperClasses.isWrapper(cls.getCanonicalName());
		return b;
	}
	
	/**
	 * Casts the specified wrapper source object to the destination class representation. This method
	 * expects the source object is one of the primitive objects and destination class is also one of the wrapper class.
	 * @param <S> Source object.
	 * @param <D> Destination class.
	 * @param <R> Returned object.
	 * @param sourceObject Source object to be recreated to the specified destination class.
	 * @param destClass Used to recreate the source object.
	 * @return Newly recreated source object with the destination class.
	 */
	public <S extends Object, D extends Class, R extends Object> R castWrapperClassFrom(
			S sourceObject, D destClass) {

		if (this.isWrapperClass(sourceObject.getClass())
				&& this.isWrapperClass(destClass)) {

			try {
				R destCls = (R) destClass.getDeclaredConstructor(String.class)
						.newInstance(sourceObject.toString());
				return destCls;
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return null;
	}
}
