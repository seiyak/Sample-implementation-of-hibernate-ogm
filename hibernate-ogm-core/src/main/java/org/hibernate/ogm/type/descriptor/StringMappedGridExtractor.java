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
package org.hibernate.ogm.type.descriptor;

import org.apache.log4j.Logger;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
import org.hibernate.type.descriptor.java.JavaTypeDescriptor;

/**
 * @author Nicolas Helleringer
 */
public class StringMappedGridExtractor<J> implements GridValueExtractor<J> {

	// private static final Log log = LoggerFactory.make();
	private static final Logger log = Logger
			.getLogger(StringMappedGridExtractor.class);

	private final GridTypeDescriptor gridTypeDescriptor;
	private final JavaTypeDescriptor<J> javaTypeDescriptor;

	public StringMappedGridExtractor(JavaTypeDescriptor<J> javaTypeDescriptor,
			GridTypeDescriptor gridTypeDescriptor) {
		this.gridTypeDescriptor = gridTypeDescriptor;
		this.javaTypeDescriptor = javaTypeDescriptor;
	}

	@Override
	public J extract(Tuple resultset, String name) {
		@SuppressWarnings("unchecked")
		final String result = (String) resultset.get(name);
		if (result == null) {
			// log.tracef( "found [null] as column [$s]", name );
			log.warn("found [null] as column [$s] " + name);
			return null;
		} else {
			final J resultJ = javaTypeDescriptor.fromString(result);
			if (log.isTraceEnabled()) {
				// log.tracef("found [$s] as column [$s]", javaTypeDescriptor
				// .extractLoggableRepresentation(resultJ), name);

				log.warn("found "
						+ javaTypeDescriptor
								.extractLoggableRepresentation(resultJ)
						+ " as column " + name);
			}
			return resultJ;
		}
	}
}
