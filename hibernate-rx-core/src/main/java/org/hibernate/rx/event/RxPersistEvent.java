package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.rx.RxSession;

/**
 * An event class for persist()
 */
public class RxPersistEvent extends PersistEvent {

	private final CompletionStage<Void> stage;

	public RxPersistEvent(
			String entityName,
			Object original,
			RxSession source,
			CompletionStage<Void> stage) {
		// FIXME: Should probably use unwrap here
		super( entityName, original, (EventSource) source );
		this.stage = stage;
	}


	public void complete() {
		stage.toCompletableFuture().complete( null );
	}

	public void completeExceptionally(Throwable t) {
		stage.toCompletableFuture().completeExceptionally( t );
	}
}