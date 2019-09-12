package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEvent;

public class RxFlushEvent extends FlushEvent {

	private CompletionStage<Void> flushStage;

	public RxFlushEvent(EventSource source, CompletionStage<Void> flushStage) {
		super( source );
		this.flushStage = flushStage;
	}

	public CompletionStage<Void> getFlushStage() {
		return flushStage;
	}
}
