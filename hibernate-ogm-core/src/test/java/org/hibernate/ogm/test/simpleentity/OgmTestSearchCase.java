package org.hibernate.ogm.test.simpleentity;

import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.SearchFactory;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;

public class OgmTestSearchCase extends OgmTestCase {

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		// TODO Auto-generated method stub
		return null;
	}

	protected SearchFactoryImplementor getSearchFactoryImpl() {
		FullTextSession s = Search.getFullTextSession(openSession());
		s.close();
		SearchFactory searchFactory = s.getSearchFactory();
		return (SearchFactoryImplementor) searchFactory;
	}
}
