package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.rx.RxSession;

/**
 * An event class for persist()
 */
public class RxDeleteEvent extends DeleteEvent {

	private final CompletionStage<Void> stage;

	public RxDeleteEvent(
			Object original,
			RxSession source,
			CompletionStage<Void> stage) {
		super( original, source.unwrap( EventSource.class ) );
		this.stage = stage;
	}

	public RxDeleteEvent(
			String entityName,
			Object original,
			RxSession source,
			CompletionStage<Void> stage) {
		super( entityName, original, source.unwrap( EventSource.class ) );
		this.stage = stage;
	}

	public CompletionStage<Void> getStage() {
		return stage;
	}
}