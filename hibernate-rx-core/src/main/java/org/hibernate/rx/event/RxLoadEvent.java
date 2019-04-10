package org.hibernate.rx.event;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.rx.RxSession;

public class RxLoadEvent<T> extends LoadEvent {

	private final CompletionStage<Optional<T>> loadStage;

	public RxLoadEvent(Object entityId, Object instanceToLoad, RxSession source, CompletionStage<Optional<T>> loadStage) {
		super( entityId, instanceToLoad, (EventSource) source );
		this.loadStage = loadStage;
	}

	public RxLoadEvent(Object entityId, String entityClassName, LockMode lockMode, RxSession source, CompletionStage<Optional<T>> loadStage) {
		super( entityId, entityClassName, lockMode, (EventSource) source );
		this.loadStage = loadStage;
	}

	public RxLoadEvent(Object entityId, String entityClassName, LockOptions lockOptions, RxSession source, CompletionStage<Optional<T>> loadStage) {
		super( entityId, entityClassName, lockOptions, (EventSource) source );
		this.loadStage = loadStage;
	}

	public RxLoadEvent(Object entityId, String entityClassName, boolean isAssociationFetch, RxSession source, CompletionStage<Optional<T>> loadStage) {
		super( entityId, entityClassName, isAssociationFetch, (EventSource) source );
		this.loadStage = loadStage;
	}

	@Override
	public void setResult(Object result) {
		Optional<T> optional = Optional.ofNullable( (T) result );
		loadStage.toCompletableFuture().complete( optional );
	}
}

