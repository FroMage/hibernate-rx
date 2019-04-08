package org.hibernate.rx;

import org.hibernate.SessionBuilder;
import org.hibernate.SessionFactory;
import org.hibernate.rx.engine.spi.RxHibernateSessionBuilderImplementor;

public interface RxHibernateSessionFactory extends SessionFactory {

	RxSession openRxSession();

	@Override
	RxHibernateSessionBuilderImplementor withOptions();

	interface RxHibernateSessionBuilder<T extends RxHibernateSessionBuilder> extends SessionBuilder<T> {

		RxSession openRxSession();
	}
}
