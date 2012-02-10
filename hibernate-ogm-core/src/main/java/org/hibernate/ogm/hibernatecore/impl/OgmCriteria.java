package org.hibernate.ogm.hibernatecore.impl;

import java.util.List;

import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.FetchMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.event.spi.EventSource;
import org.hibernate.sql.JoinType;
import org.hibernate.transform.ResultTransformer;

public class OgmCriteria implements Criteria {

	private final EventSource delegate;
	private final OgmSessionFactory factory;
	private final Class persistentClass;

	public OgmCriteria(EventSource delegate, OgmSessionFactory factory,
			Class persistentClass) {
		this.delegate = delegate;
		this.factory = factory;
		this.persistentClass = persistentClass;
	}

	@Override
	public String getAlias() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setProjection(Projection projection) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria add(Criterion criterion) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria addOrder(Order order) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setFetchMode(String associationPath, FetchMode mode)
			throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setLockMode(LockMode lockMode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setLockMode(String alias, LockMode lockMode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createAlias(String associationPath, String alias)
			throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createAlias(String associationPath, String alias,
			JoinType joinType) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createAlias(String associationPath, String alias,
			int joinType) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createAlias(String associationPath, String alias,
			JoinType joinType, Criterion withClause) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createAlias(String associationPath, String alias,
			int joinType, Criterion withClause) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath)
			throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath, JoinType joinType)
			throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath, int joinType)
			throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath, String alias)
			throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath, String alias,
			JoinType joinType) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath, String alias,
			int joinType) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath, String alias,
			JoinType joinType, Criterion withClause) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria createCriteria(String associationPath, String alias,
			int joinType, Criterion withClause) throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setResultTransformer(ResultTransformer resultTransformer) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setMaxResults(int maxResults) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setFirstResult(int firstResult) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isReadOnlyInitialized() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Criteria setReadOnly(boolean readOnly) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setFetchSize(int fetchSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setTimeout(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setCacheable(boolean cacheable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setCacheRegion(String cacheRegion) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setComment(String comment) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setFlushMode(FlushMode flushMode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Criteria setCacheMode(CacheMode cacheMode) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List list() throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScrollableResults scroll() throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScrollableResults scroll(ScrollMode scrollMode)
			throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object uniqueResult() throws HibernateException {
		// TODO Auto-generated method stub
		return null;
	}

}
