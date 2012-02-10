package org.hibernate.ogm.datastore.mapbased.impl;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WrapperClassDetectorTest {

	private WrapperClassDetector detector;

	@Before
	public void setUp() throws Exception {
		this.detector = new WrapperClassDetector();
	}

	@Test
	public void testIsWrapperClass() {
		assertFalse(this.detector.isWrapperClass(String.class));
		assertTrue(this.detector.isWrapperClass(Byte.class));
		assertTrue(this.detector.isWrapperClass(Short.class));
		assertTrue(this.detector.isWrapperClass(Integer.class));
		assertTrue(this.detector.isWrapperClass(Long.class));
		assertTrue(this.detector.isWrapperClass(Float.class));
		assertTrue(this.detector.isWrapperClass(Double.class));
		assertTrue(this.detector.isWrapperClass(Character.class));
		assertTrue(this.detector.isWrapperClass(Boolean.class));
		assertFalse(this.detector.isWrapperClass(int.class));
		assertFalse(this.detector.isWrapperClass(this.getClass()));
	}

	@Test
	public void testCastWrapperClassFrom() {

		Integer i = new Integer(1);
		Object obj = this.detector.castWrapperClassFrom(i, Byte.class);
		assertNotNull(obj);
		assertTrue(obj.getClass().getCanonicalName().equals("java.lang.Byte"));
		assertEquals((byte) 1, ((Byte) obj).byteValue());
		Short s = new Short((short) 2);
		Object obj2 = this.detector.castWrapperClassFrom(s, Byte.class);
		assertNotNull(obj2);
		assertEquals((byte) 2, ((Byte) obj2).byteValue());
	}

	@After
	public void tearDown() throws Exception {
	}

}
