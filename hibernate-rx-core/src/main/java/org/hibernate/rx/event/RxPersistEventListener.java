package org.hibernate.rx.event;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.PersistEventListener;

public interface RxPersistEventListener extends Serializable, PersistEventListener {

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 *
	 * @throws HibernateException
	 */
	void onPersist(RxPersistEvent event) throws HibernateException;

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 *
	 * @throws HibernateException
	 */
	void onPersist(RxPersistEvent event, Map<?, ?> createdAlready)
			throws HibernateException;

}