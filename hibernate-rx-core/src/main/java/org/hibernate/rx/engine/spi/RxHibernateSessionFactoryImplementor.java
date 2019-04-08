package org.hibernate.rx.engine.spi;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxHibernateSessionFactory;

public interface RxHibernateSessionFactoryImplementor extends RxHibernateSessionFactory, SessionFactoryImplementor {

	@Override
	RxHibernateSessionBuilderImplementor withOptions();

	@Override
	RxSession openRxSession() throws HibernateException;
}
