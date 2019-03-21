package org.hibernate.rx;

import java.util.function.Consumer;

import org.hibernate.Session;
import org.hibernate.rx.engine.spi.RxActionQueue;

public interface RxHibernateSession extends Session {

	@Override
	RxHibernateSessionFactory getSessionFactory();

	RxSession reactive();

	// Alternative
	void reactive(Consumer<RxSession> consumer);

	RxActionQueue getRxActionQueue();

}
