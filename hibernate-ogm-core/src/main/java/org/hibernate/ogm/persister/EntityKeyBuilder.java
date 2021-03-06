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
package org.hibernate.ogm.persister;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.type.Type;

/**
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 */
final public class EntityKeyBuilder {

	public static OgmEntityPersister DEBUG_OGM_PERSISTER;

	private EntityKeyBuilder() {
	}

	public static EntityKey fromPersisterId(final OgmEntityPersister persister,
			final Serializable id) {

		EntityKeyBuilder.DEBUG_OGM_PERSISTER = persister;
		return new EntityKey(persister.getTableName(), id,
				persister.getEntityName(),
				EntityKeyBuilder.getColumnMap(persister));
	}

	public static EntityKey fromTableNameId(final String tableName,
			final Serializable id) {
		return new EntityKey(tableName, id);
	}

	/**
	 * Once the tests are done, change the scope to private. This method is
	 * necessarily called by VoldemortDatastoreProvider.getEntityMap().
	 * 
	 * @param persister
	 * @return
	 */
	public static Map<String, String> getColumnMap(OgmEntityPersister persister) {
		Map<String, String> map = new HashMap<String, String>();

		for (String propName : persister.getPropertyNames()) {

			String columnName = persister.getPropertyColumnNames(propName)[0];
			if (!propName.equals(columnName)) {
				map.put(propName, columnName);
			}
		}

		return Collections.unmodifiableMap(map);
	}
}
