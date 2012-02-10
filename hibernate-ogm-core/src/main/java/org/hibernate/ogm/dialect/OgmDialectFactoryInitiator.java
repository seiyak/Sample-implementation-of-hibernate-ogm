/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.dialect;

import java.sql.Connection;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.service.jdbc.dialect.spi.DialectFactory;
import org.hibernate.service.spi.BasicServiceInitiator;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 */
public class OgmDialectFactoryInitiator implements
		BasicServiceInitiator<DialectFactory> {

	private static Logger log = Logger
			.getLogger(OgmDialectFactoryInitiator.class);
	private static final String HIBERNATE_DIALECT_KEYWORD = "hibernate.dialect";

	@Override
	public DialectFactory initiateService(Map configurationValues,
			ServiceRegistryImplementor registry) {

		log.info("called initiateService()");
		return new OgmDialectFactory();
	}

	@Override
	public Class<DialectFactory> getServiceInitiated() {

		log.info("called getServiceInitiated()");
		return DialectFactory.class;
	}

	private static class OgmDialectFactory implements DialectFactory {
		@Override
		public Dialect buildDialect(Map configValues, Connection connection)
				throws HibernateException {

			Object dialect = configValues.get(HIBERNATE_DIALECT_KEYWORD);

			if (dialect != null) {
				try {
					try {
						return (Dialect) Class.forName(dialect.toString())
								.newInstance();
					} catch (InstantiationException e) {
						log.error("could not instantiate the specified dialect, "
								+ dialect.toString());
					} catch (IllegalAccessException e) {
						log.error("illegal access exception: " + e.getCause());
					}
				} catch (ClassNotFoundException e) {
					log.error("could not find the specified class, "
							+ dialect.toString());
				}
			}

			return new NoopDialect();
		}
	}
}
