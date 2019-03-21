package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.impl.RxSessionImpl;

/**
 * An event class for persist()
 */
public class RxPersistEvent extends PersistEvent {

	private final CompletionStage<?> stage;

	public RxPersistEvent(
			String entityName,
			Object original,
			RxHibernateSession source,
			RxSession rxSession,
			CompletionStage<?> stage) {
		// FIXME: Should probably use unwrap here
		super( entityName, original, (EventSource) source );
		this.stage = stage;
	}

	public CompletionStage<?> getStage() {
		return stage;
	}
}