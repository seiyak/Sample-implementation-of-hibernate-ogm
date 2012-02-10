package org.hibernate.ogm.datastore.spi;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JSONedClassDetectorTest {

	JSONedClassDetector jsonedDetector;

	@Before
	public void setUp() throws Exception {
		this.jsonedDetector = new JSONedClassDetector();
	}

	@Test
	public void testIsAssignable() {

		try {
			assertTrue(this.jsonedDetector.isAssignable(Class
					.forName("java.util.Date")));
			assertFalse(this.jsonedDetector.isAssignable(this.getClass()));
			assertTrue(this.jsonedDetector.isAssignable(Class
					.forName("java.util.Calendar")));
			assertTrue(this.jsonedDetector.isAssignable(Class
					.forName("java.util.GregorianCalendar")));
			assertFalse(this.jsonedDetector.isAssignable(Integer.class));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@After
	public void tearDown() throws Exception {
	}

}
