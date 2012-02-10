package org.hibernate.ogm.test.simpleentity;

import static org.fest.assertions.Assertions.assertThat;
import static org.hibernate.ogm.test.utils.TestHelper.associationCacheSize;
import static org.hibernate.ogm.test.utils.TestHelper.entityCacheSize;

import java.io.InputStream;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.cfg.OgmConfiguration;

public class HibernateSession {

	private static final Logger log = Logger.getLogger(HibernateSession.class);
	private SessionFactory sessionFactory;
	private Session session;
	private OgmConfiguration cfg;
	public static HibernateSession HIBERNATE_SESSION = new HibernateSession();
	private Class[] annotatedClases = null;

	private HibernateSession() {

	}

	public void setAnnotatedClasses(Class[] annotatedClasses) {
		this.annotatedClases = annotatedClasses;
	}

	public Session getSession() {
		return this.session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public SessionFactory getSessionFactory() {
		return this.sessionFactory;
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session openSession() throws HibernateException {
		rebuildSessionFactory();
		session = this.sessionFactory.openSession();
		return session;
	}

	private void rebuildSessionFactory() {
		if (sessionFactory == null) {
			try {
				buildConfiguration();
			} catch (Exception e) {
				throw new HibernateException(e);
			}
		}
	}

	public void buildConfiguration() throws Exception {

		if (this.sessionFactory != null) {
			this.sessionFactory.close();
		}
		try {
			this.cfg = new OgmConfiguration();

			// Grid specific configuration
			this.cfg.setProperty(
					"hibernate.ogm.infinispan.configuration_resourcename",
					"infinispan-local.xml");
			// cfg.setProperty( "hibernate.transaction.default_factory_class",
			// JTATransactionManagerTransactionFactory.class.getName() );
			// cfg.setProperty( Environment.TRANSACTION_MANAGER_STRATEGY,
			// JBossTSStandaloneTransactionManagerLookup.class.getName() );

			// Other configurations
			// by default use the new id generator scheme...
			this.cfg.setProperty(Configuration.USE_NEW_ID_GENERATOR_MAPPINGS,
					"true");

			if (recreateSchema()) {
				this.cfg.setProperty(Environment.HBM2DDL_AUTO, "none");
			}
			for (String aPackage : getAnnotatedPackages()) {
				this.cfg.addPackage(aPackage);
			}

			if (this.annotatedClases == null
					|| this.annotatedClases.length == 0) {
				throw new RuntimeException(
						"At least one annotated class must be added to Configuration, or data store provider is not started");
			}

			for (Class<?> aClass : this.annotatedClases) {
				log.info("annotated class name: " + aClass.getCanonicalName());
				this.cfg.addAnnotatedClass(aClass);
			}

			for (String xmlFile : getXmlFiles()) {
				InputStream is = Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(xmlFile);
				this.cfg.addInputStream(is);
			}

			this.sessionFactory = this.cfg.buildSessionFactory();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String[] getAnnotatedPackages() {
		return new String[] {};
	}

	protected String[] getXmlFiles() {
		return new String[] {};
	}

	protected boolean recreateSchema() {
		return true;
	}

	protected void handleUnclosedResources() {
		if (session != null && session.isOpen()) {
			if (session.isConnected()) {
				if (session.getTransaction().isActive()) {
					session.getTransaction().rollback();
				}
			}
			session.close();
			session = null;
			log.error("unclosed session");
		} else {
			session = null;
		}
		if (sessionFactory != null && !sessionFactory.isClosed()) {
			sessionFactory.close();
			sessionFactory = null;
		}
	}

	protected void closeResources() {
		try {
			if (session != null && session.isOpen()) {
				if (session.isConnected()) {
					if (session.getTransaction().isActive()) {
						session.getTransaction().rollback();
					}
				}
				session.close();
			}
		} catch (Exception ignore) {
		}
		try {
			if (sessionFactory != null) {
				sessionFactory.close();
				sessionFactory = null;
			}
		} catch (Exception ignore) {
		}
	}

	public SessionFactoryImplementor sfi() {
		return (SessionFactoryImplementor) this.sessionFactory;
	}

	public void checkCleanCache() {
		assertThat(entityCacheSize(sessionFactory)).as(
				"Entity cache should be empty").isEqualTo(0);
		assertThat(associationCacheSize(sessionFactory)).as(
				"Association cache should be empty").isEqualTo(0);
	}
}
