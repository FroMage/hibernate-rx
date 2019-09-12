package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEntityEvent;

public class RxFlushEntityEvent extends FlushEntityEvent {

	private CompletionStage<Void> flushEntityStage;

	public RxFlushEntityEvent(
			EventSource source,
			Object entity,
			EntityEntry entry,
			CompletionStage<Void> flushEntityStage) {
		super( source, entity, entry );
		this.flushEntityStage = flushEntityStage;
	}

	public CompletionStage<Void> getFlushEntityStage() {
		return flushEntityStage;
	}
}
