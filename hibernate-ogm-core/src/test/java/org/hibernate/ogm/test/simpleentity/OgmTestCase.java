/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2010-2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.test.simpleentity;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.annotations.common.util.StringHelper;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.hibernate.testing.FailureExpected;
import org.junit.After;
import org.junit.Before;

/**
 * A base class for all OGM tests.
 * 
 * This class is a mix of SearchTestCase from HSearch 4 and OgmTestCase from the
 * Core 3.6 days It could get some love to clean this mess
 * 
 * @author Emmanuel Bernard
 * @author Hardy Ferentschik
 */
public abstract class OgmTestCase extends OgmTestBase {

	private static final Logger log = Logger.getLogger(OgmTestCase.class);
	protected static Configuration cfg;
	private static Class<?> lastTestClass;

	/**
	 * This method must be called by the derived classes.
	 * 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.setUpEmbeddedServer();
		HibernateSession.HIBERNATE_SESSION.setAnnotatedClasses(this
				.getAnnotatedClasses());
		HibernateSession.HIBERNATE_SESSION.buildConfiguration();
	}

	public Session openSession() {
		return HibernateSession.HIBERNATE_SESSION.openSession();
	}

	@After
	public void tearDown() throws Exception {
		// runSchemaDrop();
		HibernateSession.HIBERNATE_SESSION.handleUnclosedResources();
		HibernateSession.HIBERNATE_SESSION.closeResources();

		if (HibernateSession.HIBERNATE_SESSION.getSessionFactory() != null) {
			HibernateSession.HIBERNATE_SESSION.getSessionFactory().close();
			HibernateSession.HIBERNATE_SESSION.setSessionFactory(null);
		}

		this.stopEmbeddedServer();
	}

	protected abstract Class<?>[] getAnnotatedClasses();

	protected boolean recreateSchema() {
		return true;
	}

	private <T extends Annotation> T locateAnnotation(Class<T> annotationClass,
			Method runMethod) {
		T annotation = runMethod.getAnnotation(annotationClass);
		if (annotation == null) {
			annotation = getClass().getAnnotation(annotationClass);
		}
		if (annotation == null) {
			annotation = runMethod.getDeclaringClass().getAnnotation(
					annotationClass);
		}
		return annotation;
	}

	@Override
	protected void runTest() throws Throwable {
		Method runMethod = findTestMethod();
		FailureExpected failureExpected = locateAnnotation(
				FailureExpected.class, runMethod);
		try {
			super.runTest();
			if (failureExpected != null) {
				throw new FailureExpectedTestPassedException();
			}
		} catch (FailureExpectedTestPassedException t) {
			HibernateSession.HIBERNATE_SESSION.closeResources();
			throw t;
		} catch (Throwable t) {
			if (t instanceof InvocationTargetException) {
				t = ((InvocationTargetException) t).getTargetException();
			}
			if (t instanceof IllegalAccessException) {
				t.fillInStackTrace();
			}

			HibernateSession.HIBERNATE_SESSION.closeResources();

			if (failureExpected != null) {
				StringBuilder builder = new StringBuilder();
				if (StringHelper.isNotEmpty(failureExpected.message())) {
					builder.append(failureExpected.message());
				} else {
					builder.append("ignoring @FailureExpected test");
				}
				builder.append(" (").append(failureExpected.jiraKey())
						.append(")");
				log.warn(builder.toString(), t);
			} else {
				throw t;
			}
		}
	}

	@Override
	public void runBare() throws Throwable {
		Method runMethod = findTestMethod();
		final Skip skip = Skip.SKIP.determineSkipByDialect(
				Dialect.getDialect(), runMethod, this.fullTestName());
		if (skip != null) {
			skip.reportSkip(this.fullTestName());
			return;
		}

		setUp();
		try {
			runTest();
		} finally {
			tearDown();
		}
	}

	private static class FailureExpectedTestPassedException extends Exception {
		public FailureExpectedTestPassedException() {
			super("Test marked as @FailureExpected, but did not fail!");
		}
	}

	// FIXME clear cache when this happens
	protected void runSchemaGeneration() {

	}

	// FIXME clear cache when this happens
	protected void runSchemaDrop() {

	}

	public void checkCleanCache() {
		HibernateSession.HIBERNATE_SESSION.checkCleanCache();
	}
}
