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
package org.hibernate.ogm.persister;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.ogm.datastore.impl.DatastoreServices;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.StaleObjectStateException;
import org.hibernate.bytecode.instrumentation.spi.LazyPropertyInitializer;
import org.hibernate.cache.spi.CacheKey;
import org.hibernate.cache.spi.access.EntityRegionAccessStrategy;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.engine.OptimisticLockStyle;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.ValueInclusion;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Subclass;
import org.hibernate.mapping.Table;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.exception.NotSupportedException;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.loader.OgmLoader;
import org.hibernate.ogm.type.GridType;
import org.hibernate.ogm.type.TypeTranslator;
import org.hibernate.ogm.util.impl.ArrayHelper;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
import org.hibernate.ogm.util.impl.PropertyMetadataProvider;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.property.BackrefPropertyAccessor;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.EntityType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.Type;

/**
 * Use a table per concrete class strategy TODO most of the non persister code
 * SIC comes from
 * {@link org.hibernate.persister.entity.UnionSubclassEntityPersister}
 * 
 * @author Emmanuel Bernard
 */
public class OgmEntityPersister extends AbstractEntityPersister implements
		EntityPersister {

	// private static final Log log = LoggerFactory.make();
	private static final Logger log = Logger
			.getLogger(OgmEntityPersister.class);

	// not per se SQL value but a regular grid value
	private final String discriminatorSQLValue;
	private final String tableName;
	private final String[] constraintOrderedTableNames;
	private final String[][] constraintOrderedKeyColumnNames;
	private final Map<Integer, String> subclassByDiscriminatorValue = new HashMap<Integer, String>();
	private final String[] spaces;
	private final String[] subclassSpaces;
	private final GridType[] gridPropertyTypes;
	private final GridType gridVersionType;
	private final GridType gridIdentifierType;
	private Object discriminatorValue;

	// service references
	private final GridDialect gridDialect;

	public OgmEntityPersister(final PersistentClass persistentClass,
			final EntityRegionAccessStrategy cacheAccessStrategy,
			final SessionFactoryImplementor factory, final Mapping mapping)
			throws HibernateException {
		super(persistentClass, cacheAccessStrategy, factory);
		if (log.isTraceEnabled()) {
			// log.tracef("Creating OgmEntityPersister for $s",
			// persistentClass.getClassName());
			log.info("Creating OgmEntityPersister for "
					+ persistentClass.getClassName());
		}
		ServiceRegistryImplementor serviceRegistry = factory
				.getServiceRegistry();
		this.gridDialect = serviceRegistry.getService(DatastoreServices.class)
				.getGridDialect();

		tableName = persistentClass.getTable().getQualifiedName(
				factory.getDialect(),
				factory.getSettings().getDefaultCatalogName(),
				factory.getSettings().getDefaultSchemaName());
		discriminatorValue = persistentClass.getSubclassId();
		discriminatorSQLValue = String.valueOf(persistentClass.getSubclassId());

		// SUBCLASSES

		// We do not need a discriminator as each entity type will be in its own
		// key "space" roughly like a
		// UnionSubclassEntityPersister / table-per-concrete-class
		// implementation
		subclassByDiscriminatorValue.put(persistentClass.getSubclassId(),
				persistentClass.getEntityName());
		if (persistentClass.isPolymorphic()) {
			@SuppressWarnings("unchecked")
			Iterator<Subclass> iter = persistentClass.getSubclassIterator();
			int k = 1;
			while (iter.hasNext()) {
				Subclass sc = iter.next();
				subclassByDiscriminatorValue.put(sc.getSubclassId(),
						sc.getEntityName());
			}
		}

		// SPACES
		// TODO: i'm not sure, but perhaps we should exclude
		// abstract denormalized tables?

		int spacesSize = 1 + persistentClass.getSynchronizedTables().size();
		spaces = new String[spacesSize];
		spaces[0] = tableName;
		@SuppressWarnings("unchecked")
		Iterator<String> syncTablesIter = persistentClass
				.getSynchronizedTables().iterator();
		for (int i = 1; i < spacesSize; i++) {
			spaces[i] = syncTablesIter.next();
		}

		HashSet<String> subclassTables = new HashSet<String>();
		Iterator<Table> tableIter = persistentClass
				.getSubclassTableClosureIterator();
		while (tableIter.hasNext()) {
			Table table = tableIter.next();
			subclassTables.add(table.getQualifiedName(factory.getDialect(),
					factory.getSettings().getDefaultCatalogName(), factory
							.getSettings().getDefaultSchemaName()));
		}
		subclassSpaces = ArrayHelper.toStringArray(subclassTables);

		if (isMultiTable()) {
			int idColumnSpan = getIdentifierColumnSpan();
			ArrayList<String> tableNames = new ArrayList<String>();
			ArrayList<String[]> keyColumns = new ArrayList<String[]>();
			if (!isAbstract()) {
				tableNames.add(tableName);
				keyColumns.add(getIdentifierColumnNames());
			}
			@SuppressWarnings("unchecked")
			Iterator<Table> iter = persistentClass
					.getSubclassTableClosureIterator();
			while (iter.hasNext()) {
				Table tab = iter.next();
				if (!tab.isAbstractUnionTable()) {
					String tableName = tab.getQualifiedName(factory
							.getDialect(), factory.getSettings()
							.getDefaultCatalogName(), factory.getSettings()
							.getDefaultSchemaName());
					tableNames.add(tableName);
					String[] key = new String[idColumnSpan];
					@SuppressWarnings("unchecked")
					Iterator<Column> citer = tab.getPrimaryKey()
							.getColumnIterator();
					for (int k = 0; k < idColumnSpan; k++) {
						key[k] = citer.next().getQuotedName(
								factory.getDialect());
					}
					keyColumns.add(key);
				}
			}

			constraintOrderedTableNames = ArrayHelper.toStringArray(tableNames);
			constraintOrderedKeyColumnNames = ArrayHelper
					.to2DStringArray(keyColumns);
		} else {
			constraintOrderedTableNames = new String[] { tableName };
			constraintOrderedKeyColumnNames = new String[][] { getIdentifierColumnNames() };
		}

		initPropertyPaths(mapping);

		// Grid related metadata
		TypeTranslator typeTranslator = serviceRegistry
				.getService(TypeTranslator.class);
		final Type[] types = getPropertyTypes();
		final int length = types.length;
		gridPropertyTypes = new GridType[length];
		for (int index = 0; index < length; index++) {
			gridPropertyTypes[index] = typeTranslator.getType(types[index]);
		}
		gridVersionType = typeTranslator.getType(getVersionType());
		gridIdentifierType = typeTranslator.getType(getIdentifierType());
	}

	// FIXME finish implement postInstantiate
	public void postInstantiate() {
		createLoaders();
		// createUniqueKeyLoaders();
		createQueryLoader();
	}

	public GridType getGridIdentifierType() {
		return gridIdentifierType;
	}

	/**
	 * This snapshot is meant to be used when updating data.
	 */
	@Override
	public Object[] getDatabaseSnapshot(Serializable id,
			SessionImplementor session) throws HibernateException {

		if (log.isTraceEnabled()) {
			log.trace("Getting current persistent state for: "
					+ MessageHelper.infoString(this, id, getFactory()));
		}

		// snapshot is a Map in the end
		final Tuple resultset = getResultsetById(id);

		// if there is no resulting row, return null
		if (resultset == null || resultset.getSnapshot().isEmpty()) {
			return null;
		}
		// otherwise return the "hydrated" state (ie. associations are not
		// resolved)
		GridType[] types = gridPropertyTypes;
		Object[] values = new Object[types.length];
		boolean[] includeProperty = getPropertyUpdateability();
		for (int i = 0; i < types.length; i++) {
			if (includeProperty[i]) {
				values[i] = types[i].hydrate(resultset,
						getPropertyAliases("", i), session, null); // null owner
																	// ok??
			}
		}
		return values;
	}

	private Tuple getResultsetById(Serializable id) {
		final EntityKey key = EntityKeyBuilder.fromPersisterId(this, id);
		final Tuple resultset = gridDialect.getTuple(key);
		return resultset;
	}

	@Override
	public Object initializeLazyProperty(String fieldName, Object entity,
			SessionImplementor session) throws HibernateException {

		final Serializable id = session.getContextEntityIdentifier(entity);

		final EntityEntry entry = session.getPersistenceContext().getEntry(
				entity);
		if (entry == null) {
			throw new HibernateException(
					"entity is not associated with the session: " + id);
		}

		if (log.isTraceEnabled()) {
			log.trace("initializing lazy properties of: "
					+ MessageHelper.infoString(this, id, getFactory())
					+ ", field access: " + fieldName);
		}

		if (hasCache()) {
			CacheKey cacheKey = session.generateCacheKey(id,
					getIdentifierType(), getEntityName());
			Object ce = getCacheAccessStrategy().get(cacheKey,
					session.getTimestamp());
			if (ce != null) {
				CacheEntry cacheEntry = (CacheEntry) getCacheEntryStructure()
						.destructure(ce, getFactory());
				if (!cacheEntry.areLazyPropertiesUnfetched()) {
					// note early exit here:
					return initializeLazyPropertiesFromCache(fieldName, entity,
							session, entry, cacheEntry);
				}
			}
		}

		return initializeLazyPropertiesFromDatastore(fieldName, entity,
				session, id, entry);

	}

	// FIXME cache should use Core Types or Grid Types?
	// Make superclasses method protected??
	private Object initializeLazyPropertiesFromCache(final String fieldName,
			final Object entity, final SessionImplementor session,
			final EntityEntry entry, final CacheEntry cacheEntry) {
		throw new NotSupportedException("OGM-9",
				"Lazy properties not supported in OGM");
	}

	private Object initializeLazyPropertiesFromDatastore(
			final String fieldName, final Object entity,
			final SessionImplementor session, final Serializable id,
			final EntityEntry entry) {
		throw new NotSupportedException("OGM-9",
				"Lazy properties not supported in OGM");
	}

	/**
	 * Retrieve the version number
	 */
	@Override
	public Object getCurrentVersion(Serializable id, SessionImplementor session)
			throws HibernateException {

		if (log.isTraceEnabled()) {
			log.trace("Getting version: "
					+ MessageHelper.infoString(this, id, getFactory()));
		}
		final Tuple resultset = getResultsetById(id);

		if (resultset == null) {
			return null;
		} else {
			return gridVersionType.nullSafeGet(resultset,
					getVersionColumnName(), session, null);
		}
	}

	@Override
	public Object forceVersionIncrement(Serializable id, Object currentVersion,
			SessionImplementor session) {
		if (!isVersioned()) {
			throw new AssertionFailure(
					"cannot force version increment on non-versioned entity");
		}

		if (isVersionPropertyGenerated()) {
			// the difficulty here is exactly what do we update in order to
			// force the version to be incremented in the db...
			throw new HibernateException(
					"LockMode.FORCE is currently not supported for generated version properties");
		}

		Object nextVersion = getVersionType().next(currentVersion, session);
		if (log.isTraceEnabled()) {
			log.trace("Forcing version increment ["
					+ MessageHelper.infoString(this, id, getFactory())
					+ "; "
					+ getVersionType().toLoggableString(currentVersion,
							getFactory())
					+ " -> "
					+ getVersionType().toLoggableString(nextVersion,
							getFactory()) + "]");
		}

		/*
		 * We get the value from the grid and compare the version values before
		 * putting the next version in Contrary to the database version, there
		 * is TODO should we use cache.replace() it seems more expensive to pass
		 * the resultset around "just" the atomicity of the operation
		 */
		final EntityKey key = EntityKeyBuilder.fromPersisterId(this, id);
		final Tuple resultset = gridDialect.getTuple(key);
		checkVersionAndRaiseSOSE(id, currentVersion, session, resultset);
		gridVersionType.nullSafeSet(resultset, nextVersion,
				new String[] { getVersionColumnName() }, session);
		gridDialect.updateTuple(resultset, key);
		return nextVersion;
	}

	// TODO move that code to the EntityLoader as it is in
	// AbstractEntityPersister?
	@Override
	public Object loadByUniqueKey(String propertyName, Object uniqueKey,
			SessionImplementor session) throws HibernateException {
		// we get the property type for an associated entity
		final GridType gridUniqueKeyType = getUniqueKeyTypeFromAssociatedEntity(propertyName);
		// get the associated property index (to get its column names)
		final int propertyIndex = getPropertyIndex(propertyName);
		// find the ids per unique property name
		PropertyMetadataProvider metadataProvider = new PropertyMetadataProvider()
				.tableName(getTableName()).gridDialect(gridDialect)
				.key(uniqueKey).keyGridType(gridUniqueKeyType)
				.keyColumnNames(getPropertyColumnNames(propertyIndex))
				.session(session);
		final Association ids = metadataProvider.getCollectionMetadata();

		if (ids == null || ids.size() == 0) {
			return null;
		} else if (ids.size() == 1) {
			// EntityLoader#loadByUniqueKey uses a null object and LockMode.NONE
			// there is only one element in the list, so get the first
			Tuple tuple = ids.get(ids.getKeys().iterator().next());
			final Serializable id = (Serializable) getGridIdentifierType()
					.nullSafeGet(tuple, getIdentifierColumnNames(), session,
							null);
			return load(id, null, LockMode.NONE, session);
		} else {
			throw new AssertionFailure(
					"Loading by unique key but finding several matches: table:"
							+ getTableName() + " property: " + propertyName
							+ " value: " + uniqueKey);
		}
	}

	private GridType getUniqueKeyTypeFromAssociatedEntity(String propertyName) {
		GridType gridUniqueKeyType;// get the unique key type and if it's an
									// entity type, get it's identifier type
		final Type uniqueKeyType = propertyMapping.toType(propertyName);
		if (uniqueKeyType.isEntityType()) {
			String className = ((EntityType) uniqueKeyType)
					.getAssociatedEntityName();
			// we run under the assumption that we are fully in an OGM world
			final OgmEntityPersister entityPersister = (OgmEntityPersister) getFactory()
					.getEntityPersister(className);
			gridUniqueKeyType = entityPersister.getGridIdentifierType();
		} else {
			throw new AssertionFailure("loadByUniqueKey on a non EntityType:"
					+ propertyName);
		}
		return gridUniqueKeyType;
	}

	@Override
	protected void createLoaders() {
		Map<Object, Object> loaders = getLoaders();
		loaders.put(LockMode.NONE, createEntityLoader(LockMode.NONE));

		UniqueEntityLoader readLoader = createEntityLoader(LockMode.READ);
		loaders.put(LockMode.READ, readLoader);

		// TODO: inexact, what we really need to know is: are any outer joins
		// used?
		boolean disableForUpdate = getSubclassTableSpan() > 1
				&& hasSubclasses()
				&& !getFactory().getDialect().supportsOuterJoinForUpdate();

		loaders.put(LockMode.UPGRADE, disableForUpdate ? readLoader
				: createEntityLoader(LockMode.UPGRADE));
		loaders.put(LockMode.UPGRADE_NOWAIT, disableForUpdate ? readLoader
				: createEntityLoader(LockMode.UPGRADE_NOWAIT));
		loaders.put(LockMode.FORCE, disableForUpdate ? readLoader
				: createEntityLoader(LockMode.FORCE));
		loaders.put(LockMode.PESSIMISTIC_READ, disableForUpdate ? readLoader
				: createEntityLoader(LockMode.PESSIMISTIC_READ));
		loaders.put(LockMode.PESSIMISTIC_WRITE, disableForUpdate ? readLoader
				: createEntityLoader(LockMode.PESSIMISTIC_WRITE));
		loaders.put(
				LockMode.PESSIMISTIC_FORCE_INCREMENT,
				disableForUpdate ? readLoader
						: createEntityLoader(LockMode.PESSIMISTIC_FORCE_INCREMENT));
		loaders.put(LockMode.OPTIMISTIC,
				createEntityLoader(LockMode.OPTIMISTIC));
		loaders.put(LockMode.OPTIMISTIC_FORCE_INCREMENT,
				createEntityLoader(LockMode.OPTIMISTIC_FORCE_INCREMENT));

		// FIXME handle cascading merge and refresh
		loaders.put("merge", createEntityLoader(LockMode.READ)
		// new CascadeEntityLoader( this, CascadingAction.MERGE, getFactory() )
		);
		loaders.put("refresh", createEntityLoader(LockMode.READ)
		// new CascadeEntityLoader( this, CascadingAction.REFRESH, getFactory()
		// )
		);
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		// FIXME add support to lock mode and loadQueryInfluencers
		return new OgmLoader(new OgmEntityPersister[] { this });
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockOptions lockOptions,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		// FIXME add support to lock options and loadQueryInfluencers
		return new OgmLoader(new OgmEntityPersister[] { this });
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode)
			throws MappingException {
		return createEntityLoader(lockMode, LoadQueryInfluencers.NONE);
	}

	// TODO verify what to do with #check: Expectation seems to be very JDBC
	// centric

	/**
	 * Unmarshall the fields of a persistent instance from a result set, without
	 * resolving associations or collections. Question: should this really be
	 * here, or should it be sent back to Loader?
	 */
	public Object[] hydrate(final Tuple resultset, final Serializable id,
			final Object object, final Loadable rootLoadable,
			// We probably don't need suffixedColumns, use column names instead
			// final String[][] suffixedPropertyColumns,
			final boolean allProperties, final SessionImplementor session)
			throws HibernateException {

		if (log.isTraceEnabled()) {
			log.trace("Hydrating entity: "
					+ MessageHelper.infoString(this, id, getFactory()));
		}

		final OgmEntityPersister rootPersister = (OgmEntityPersister) rootLoadable;

		final boolean hasDeferred = rootPersister.hasSequentialSelect();
		boolean sequentialSelectEmpty = false;
		if (hasDeferred) {
			// note: today we don't have sequential select in OGM
			// check AbstractEntityPersister#hydrate for the detail
		}

		final String[] propNames = getPropertyNames();
		final Type[] types = getPropertyTypes();
		final Object[] values = new Object[types.length];
		final boolean[] laziness = getPropertyLaziness();
		final String[] propSubclassNames = getSubclassPropertySubclassNameClosure();
		final boolean[] propertySelectable = getPropertySelectable();
		for (int i = 0; i < types.length; i++) {
			values[i] = hydrateValue(resultset, session, object, i,
					propertySelectable, allProperties, laziness, hasDeferred,
					rootPersister, propNames, propSubclassNames,
					sequentialSelectEmpty);
		}
		return values;
	}

	private Object hydrateValue(Tuple resultset, SessionImplementor session,
			Object object, int index, boolean[] propertySelectable,
			boolean allProperties, boolean[] laziness, boolean hasDeferred,
			OgmEntityPersister rootPersister, String[] propNames,
			String[] propSubclassNames, boolean sequentialSelectEmpty) {
		Object value;
		if (!propertySelectable[index]) {
			value = BackrefPropertyAccessor.UNKNOWN;
		} else if (allProperties || !laziness[index]) {
			// decide which ResultSet to get the property value from:
			final boolean propertyIsDeferred = hasDeferred
					&& rootPersister.isSubclassPropertyDeferred(
							propNames[index], propSubclassNames[index]);
			if (propertyIsDeferred && sequentialSelectEmpty) {
				value = null;
			} else {
				// FIXME We don't handle deferred property yet
				// final ResultSet propertyResultSet = propertyIsDeferred ?
				// sequentialResultSet : rs;
				GridType[] gridTypes = gridPropertyTypes;
				final String[] cols;
				if (propertyIsDeferred) {
					cols = getPropertyAliases("", index);
				} else {
					// TODO What to do?
					// : suffixedPropertyColumns[i];
					cols = getPropertyAliases("", index);
				}
				value = gridTypes[index].hydrate(resultset, cols, session,
						object); // null owner ok??
			}
		} else {
			value = LazyPropertyInitializer.UNFETCHED_PROPERTY;
		}
		return value;
	}

	@Override
	public String[] getPropertyAliases(String suffix, int i) {
		// TODO do something about suffixes
		return getPropertyColumnNames(i);
	}

	@Override
	protected boolean useInsertSelectIdentity() {
		return false;
	}

	@Override
	protected Serializable insert(final Object[] fields,
			final boolean[] notNull, String sql, final Object object,
			final SessionImplementor session) throws HibernateException {
		throw new HibernateException("Cannot use a database generator with OGM");
	}

	@Override
	protected LockingStrategy generateLocker(LockMode lockMode) {
		return gridDialect.getLockingStrategy(this, lockMode);
	}

	/**
	 * Update an object
	 */
	@Override
	public void update(final Serializable id, final Object[] fields,
			final int[] dirtyFields, final boolean hasDirtyCollection,
			final Object[] oldFields, final Object oldVersion,
			final Object object, final Object rowId,
			final SessionImplementor session) throws HibernateException {

		// note: dirtyFields==null means we had no snapshot, and we couldn't get
		// one using select-before-update
		// oldFields==null just means we had no snapshot to begin with (we might
		// have used select-before-update to get the dirtyFields)

		// TODO support "multi table" entities
		final boolean[] tableUpdateNeeded = getTableUpdateNeeded(dirtyFields,
				hasDirtyCollection);
		final int span = getTableSpan();

		final boolean[] propsToUpdate;
		final String[] updateStrings;
		EntityEntry entry = session.getPersistenceContext().getEntry(object);

		// Ensure that an immutable or non-modifiable entity is not being
		// updated unless it is
		// in the process of being deleted.
		if (entry == null && !isMutable()) {
			throw new IllegalStateException(
					"Updating immutable entity that is not in session yet!");
		}
		// we always use a dynamicUpdate model for Infinispan
		if ((
		// getEntityMetamodel().isDynamicUpdate() &&
		dirtyFields != null)) {

			propsToUpdate = getPropertiesToUpdate(dirtyFields,
					hasDirtyCollection);
			// don't need to check laziness (dirty checking algorithm handles
			// that)
		} else if (!isModifiableEntity(entry)) {
			// TODO does that apply to OGM?
			// We need to generate UPDATE SQL when a non-modifiable entity
			// (e.g., read-only or immutable)
			// needs:
			// - to have references to transient entities set to null before
			// being deleted
			// - to have version incremented do to a "dirty" association
			// If dirtyFields == null, then that means that there are no dirty
			// properties to
			// to be updated; an empty array for the dirty fields needs to be
			// passed to
			// getPropertiesToUpdate() instead of null.
			propsToUpdate = getPropertiesToUpdate(
					(dirtyFields == null ? ArrayHelper.EMPTY_INT_ARRAY
							: dirtyFields), hasDirtyCollection);
			// don't need to check laziness (dirty checking algorithm handles
			// that)
		} else {
			// For the case of dynamic-update="false", or no snapshot, we update
			// all properties
			// TODO handle lazy
			propsToUpdate = getPropertyUpdateability(object);
		}

		final SessionFactoryImplementor factory = getFactory();
		if (log.isTraceEnabled()) {
			log.trace("Updating entity: "
					+ MessageHelper.infoString(this, id, factory));
			if (isVersioned()) {
				log.trace("Existing version: " + oldVersion
						+ " -> New version: " + fields[getVersionProperty()]);
			}
		}

		for (int j = 0; j < span; j++) {
			// Now update only the tables with dirty properties (and the table
			// with the version number)
			if (tableUpdateNeeded[j]) {
				final EntityKey key = EntityKeyBuilder
						.fromPersisterId(this, id);
				Tuple resultset = gridDialect.getTuple(key);
				final boolean useVersion = j == 0 && isVersioned();

				resultset = createNewResultSetIfNull(key, resultset, id,
						session);

				final EntityMetamodel entityMetamodel = getEntityMetamodel();

				// Write any appropriate versioning conditional parameters
				if (useVersion
						&& entityMetamodel.getOptimisticLockStyle() == OptimisticLockStyle.VERSION) {
					if (checkVersion(propsToUpdate)) {
						checkVersionAndRaiseSOSE(id, oldVersion, session,
								resultset);
					}
				} else if (isAllOrDirtyOptLocking() && oldFields != null) {
					boolean[] versionability = getPropertyVersionability(); // TODO:
																			// is
																			// this
																			// really
																			// necessary????
					boolean[] includeOldField = entityMetamodel
							.getOptimisticLockStyle() == OptimisticLockStyle.ALL ? getPropertyUpdateability()
							: propsToUpdate;
					// TODO do a diff on the properties value from resultset and
					// the dirty value
					GridType[] types = gridPropertyTypes;

					for (int i = 0; i < entityMetamodel.getPropertySpan(); i++) {
						boolean include = includeOldField[i]
								&& isPropertyOfTable(i, j) && versionability[i]; // TODO:
																					// is
																					// this
																					// really
																					// necessary????
						if (include) {
							final GridType type = types[i];
							// FIXME what do do with settable?
							boolean[] settable = type.toColumnNullness(
									oldFields[i], factory);
							final Object snapshotValue = type.nullSafeGet(
									resultset, getPropertyColumnNames(i),
									session, object);
							comparePropertyAndRaiseSOSE(id, oldFields[i],
									factory, !type.isEqual(oldFields,
											snapshotValue, factory));

						}
					}
				}

				// dehydrate
				dehydrate(resultset, fields, propsToUpdate,
						getPropertyColumnUpdateable(), j, id, session);
				gridDialect.updateTuple(resultset, key);
			}
		}
	}

	// Copied from AbstractEntityPersister
	private boolean isAllOrDirtyOptLocking() {
		EntityMetamodel entityMetamodel = getEntityMetamodel();
		return entityMetamodel.getOptimisticLockStyle() == OptimisticLockStyle.DIRTY
				|| entityMetamodel.getOptimisticLockStyle() == OptimisticLockStyle.ALL;
	}

	private void comparePropertyAndRaiseSOSE(Serializable id, Object oldField,
			SessionFactoryImplementor factory, boolean b) {
		// TODO support other entity modes
		if (b) {
			if (factory.getStatistics().isStatisticsEnabled()) {
				factory.getStatisticsImplementor().optimisticFailure(
						getEntityName());
			}
			throw new StaleObjectStateException(getEntityName(), id);
		}
	}

	public void checkVersionAndRaiseSOSE(Serializable id, Object oldVersion,
			SessionImplementor session, Tuple resultset) {
		final Object resultSetVersion = gridVersionType.nullSafeGet(resultset,
				getVersionColumnName(), session, null);
		final SessionFactoryImplementor factory = getFactory();
		if (!gridVersionType.isEqual(oldVersion, resultSetVersion, factory)) {
			if (factory.getStatistics().isStatisticsEnabled()) {
				factory.getStatisticsImplementor().optimisticFailure(
						getEntityName());
			}
			throw new StaleObjectStateException(getEntityName(), id);
		}
	}

	private void dehydrate(Tuple resultset, final Object[] fields,
			boolean[] includeProperties, boolean[][] includeColumns,
			int tableIndex, Serializable id, SessionImplementor session) {
		new EntityDehydrator().fields(fields)
				.gridPropertyTypes(gridPropertyTypes)
				.gridIdentifierType(gridIdentifierType).id(id)
				.includeColumns(includeColumns)
				.includeProperties(includeProperties).persister(this)
				.resultset(resultset).session(session).tableIndex(tableIndex)
				.gridDialect(gridDialect).dehydrate();
	}

	// TODO copy of AbstractEntityPersister#checkVersion due to visibility
	private boolean checkVersion(final boolean[] includeProperty) {
		return includeProperty[getVersionProperty()]
				|| getEntityMetamodel().getPropertyUpdateGenerationInclusions()[getVersionProperty()] != ValueInclusion.NONE;

	}

	// TODO make AbstractEntityPersister#isModifiableEntity protected instead
	private boolean isModifiableEntity(EntityEntry entry) {
		return (entry == null ? isMutable() : entry.isModifiableEntity());
	}

	@Override
	public Serializable insert(Object[] fields, Object object,
			SessionImplementor session) throws HibernateException {
		throw new HibernateException(
				"Identifier values generated by the database are not supported in Hibernate OGM");
	}

	@Override
	public void insert(Serializable id, Object[] fields, Object object,
			SessionImplementor session) throws HibernateException {

		final int span = getTableSpan();
		// insert operations are always dynamic in OGM
		boolean[] propertiesToInsert = getPropertiesToInsert(fields);
		for (int j = 0; j < span; j++) {
			if (isInverseTable(j)) {
				return;
			}

			// note: it is conceptually possible that a UserType could map null
			// to
			// a non-null value, so the following is arguable:
			if (isNullableTable(j) && isAllNull(fields, j)) {
				return;
			}

			// if ( log.isTraceEnabled() ) {
			log.info("Inserting entity: "
					+ MessageHelper.infoString(this, id, getFactory()));
			if (j == 0 && isVersioned()) {
				log.trace("Version: " + Versioning.getVersion(fields, this));
			}
			// }

			final EntityKey key = EntityKeyBuilder.fromPersisterId(this, id);
			Tuple resultset = gridDialect.getTuple(key);
			// add the discriminator
			if (j == 0) {
				if (resultset != null) {
					throw new HibernateException(
							"trying to insert an already existing entity: "
									+ MessageHelper.infoString(this, id,
											getFactory()));
				}
				// TODO add discriminator

			}

			resultset = createNewResultSetIfNull(key, resultset, id, session);

			// dehydrate
			dehydrate(resultset, fields, propertiesToInsert,
					getPropertyColumnInsertable(), j, id, session);
			gridDialect.updateTuple(resultset, key);
		}
	}

	private Tuple createNewResultSetIfNull(EntityKey key, Tuple resultset,
			Serializable id, SessionImplementor session) {
		if (resultset == null) {
			resultset = gridDialect.createTuple(key);
			gridIdentifierType.nullSafeSet(resultset, id,
					getIdentifierColumnNames(), session);
		}
		return resultset;
	}

	// TODO AbstractEntityPersister#isAllNull copied because of visibility
	private boolean isAllNull(Object[] array, int tableNumber) {
		for (int i = 0; i < array.length; i++) {
			if (isPropertyOfTable(i, tableNumber) && array[i] != null) {
				return false;
			}
		}
		return true;
	}

	public void delete(Serializable id, Object version, Object object,
			SessionImplementor session) throws HibernateException {
		final int span = getTableSpan();
		if (span > 1)
			throw new HibernateException(
					"Hibernate OGM does not yet support entities spanning multiple tables");
		final EntityMetamodel entityMetamodel = getEntityMetamodel();
		boolean isImpliedOptimisticLocking = !entityMetamodel.isVersioned()
				&& isAllOrDirtyOptLocking();
		Object[] loadedState = null;
		if (isImpliedOptimisticLocking) {
			// need to treat this as if it where optimistic-lock="all" (dirty
			// does *not* make sense);
			// first we need to locate the "loaded" state
			//
			// Note, it potentially could be a proxy, so
			// doAfterTransactionCompletion the location the safe way...
			org.hibernate.engine.spi.EntityKey key = session.generateEntityKey(
					id, this);
			Object entity = session.getPersistenceContext().getEntity(key);
			if (entity != null) {
				EntityEntry entry = session.getPersistenceContext().getEntry(
						entity);
				loadedState = entry.getLoadedState();
			}
		}

		final EntityKey key = EntityKeyBuilder.fromPersisterId(this, id);
		final Tuple resultset = gridDialect.getTuple(key);
		final SessionFactoryImplementor factory = getFactory();
		if (isImpliedOptimisticLocking && loadedState != null) {
			// we need to utilize dynamic delete statements
			for (int j = span - 1; j >= 0; j--) {
				boolean[] versionability = getPropertyVersionability();

				// TODO do a diff on the properties value from resultset
				GridType[] types = gridPropertyTypes;

				for (int i = 0; i < entityMetamodel.getPropertySpan(); i++) {
					boolean include = isPropertyOfTable(i, j)
							&& versionability[i];
					if (include) {
						final GridType type = types[i];
						final Object snapshotValue = type.nullSafeGet(
								resultset, getPropertyColumnNames(i), session,
								object);
						// TODO support other entity modes
						if (!type.isEqual(loadedState[i], snapshotValue,
								factory)) {
							if (factory.getStatistics().isStatisticsEnabled()) {
								factory.getStatisticsImplementor()
										.optimisticFailure(getEntityName());
							}
							throw new StaleObjectStateException(
									getEntityName(), id);
						}
					}
				}
			}
		} else {
			if (entityMetamodel.isVersioned()) {
				checkVersionAndRaiseSOSE(id, version, session, resultset);
			}
		}

		for (int j = span - 1; j >= 0; j--) {
			if (isInverseTable(j)) {
				return;
			}
			if (log.isTraceEnabled()) {
				log.trace("Deleting entity: "
						+ MessageHelper.infoString(this, id, factory));
				if (j == 0 && isVersioned()) {
					log.trace("Version: " + version);
				}
			}

			// delete association information
			// needs to be executed before the tuple removal because the
			// AtomicMap in ISPN is cleared upon removal
			new EntityDehydrator().gridDialect(gridDialect)
					.gridPropertyTypes(gridPropertyTypes)
					.gridIdentifierType(gridIdentifierType).id(id)
					.persister(this).resultset(resultset).session(session)
					.tableIndex(j).onlyRemovePropertyMetadata().dehydrate();

			gridDialect.removeTuple(key);
		}

	}

	@Override
	protected int[] getSubclassColumnTableNumberClosure() {
		return new int[getSubclassColumnClosure().length];
	}

	@Override
	protected int[] getSubclassFormulaTableNumberClosure() {
		return new int[getSubclassFormulaClosure().length];
	}

	@Override
	public String getDiscriminatorSQLValue() {
		return discriminatorSQLValue;
	}

	@Override
	public String[] getConstraintOrderedTableNameClosure() {
		return constraintOrderedTableNames;
	}

	@Override
	public String[][] getContraintOrderedTableKeyColumnClosure() {
		return constraintOrderedKeyColumnNames;
	}

	@Override
	public String getSubclassTableName(int j) {
		if (j != 0)
			throw new AssertionFailure("only one table");
		return tableName;
	}

	@Override
	protected String[] getSubclassTableKeyColumns(int j) {
		if (j != 0)
			throw new AssertionFailure("only one table");
		return getIdentifierColumnNames();
	}

	@Override
	protected boolean isClassOrSuperclassTable(int j) {
		if (j != 0)
			throw new AssertionFailure("only one table");
		return true;
	}

	@Override
	protected int getSubclassTableSpan() {
		return 1;
	}

	@Override
	protected int getTableSpan() {
		return 1;
	}

	@Override
	protected boolean isTableCascadeDeleteEnabled(int j) {
		return false;
	}

	@Override
	protected String getTableName(int j) {
		return tableName;
	}

	@Override
	protected String[] getKeyColumns(int j) {
		return getIdentifierColumnNames();
	}

	@Override
	protected boolean isPropertyOfTable(int property, int j) {
		return true;
	}

	@Override
	protected int[] getPropertyTableNumbersInSelect() {
		return new int[getPropertySpan()];
	}

	@Override
	protected int[] getPropertyTableNumbers() {
		return new int[getPropertySpan()];
	}

	@Override
	protected int getSubclassPropertyTableNumber(int i) {
		return 0;
	}

	@Override
	protected String filterFragment(String alias) throws MappingException {
		// TODO support filter in OGM??? How???
		return "";
		// return hasWhere() ?
		// " and " + getSQLWhereString(alias) :
		// "";
	}

	@Override
	public String getSubclassPropertyTableName(int i) {
		return getTableName();// ie. the subquery! yuck!
	}

	// FIXME useful?
	@Override
	public String fromTableFragment(String alias) {
		return getTableName() + ' ' + alias;
	}

	@Override
	public String getPropertyTableName(String propertyName) {
		// TODO: check this....
		return getTableName();
	}

	@Override
	public String getTableName() {
		// FIXME it should be the subquery ie include the subclasses as well in
		// SQL (union logic)
		return tableName;
	}

	@Override
	public Type getDiscriminatorType() {
		return IntegerType.INSTANCE;
	}

	@Override
	public Object getDiscriminatorValue() {
		return discriminatorValue;
	}

	@Override
	public String getSubclassForDiscriminatorValue(Object value) {
		return subclassByDiscriminatorValue.get(value);
	}

	@Override
	public Serializable[] getPropertySpaces() {
		return spaces;
	}
}
