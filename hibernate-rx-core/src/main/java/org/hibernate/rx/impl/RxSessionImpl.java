package org.hibernate.rx.impl;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import javax.persistence.EntityTransaction;

import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.rx.ReactiveTransaction;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.StateControl;
import org.hibernate.service.ServiceRegistry;

public class RxSessionImpl implements RxSession {

	// Might make sense to have a service or delegator for this
	private Executor executor = ForkJoinPool.commonPool();

	private final RxHibernateSessionFactory factory;
	private final RxHibernateSession rxHibernateSession;
	private CompletionStage<ReactiveTransaction> txStage;

	public <T> RxSessionImpl(RxHibernateSessionFactory factory, RxHibernateSession session) {
		this.factory = factory;
		this.rxHibernateSession = session;
	}

	@Override
	public CompletionStage<ReactiveTransaction> beginTransaction() {
		txStage = initTxStage();
		return txStage;
	}

	private void waitForABit() {
		try {
			Thread.sleep( 60 * 60 * 1000L );
		}
		catch (InterruptedException e) {
			throw new RuntimeException( e );
		}
	}

	private CompletionStage<ReactiveTransaction> initTxStage() {
		return CompletableFuture.supplyAsync( () -> {
			return new ReactiveTransactionImpl( this, rxHibernateSession.beginTransaction() );
		} );
	}

	@Override
	public CompletionStage<ReactiveTransaction> beginTransaction(Consumer<RxSession> consumer) {
		throw new RuntimeException( "Remove this" );
	}

	@Override
	public CompletionStage<ReactiveTransaction> inTransaction(Consumer<RxSession> consumer) {
		txStage = txStage.thenApply( tx -> {
			consumer.accept( this );
			return tx;
		} );
		return txStage;
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id) {
		return CompletableFuture.supplyAsync( () -> {
			T result = rxHibernateSession.find( entityClass, id );
			return Optional.ofNullable( result );
		} );
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		CompletionStage<Void> newTx = txStage.thenApply( (tx) -> {
			rxHibernateSession.persist( entity );
			return null;
		} );
		return newTx;
	}

	private ExceptionConverter exceptionConverter() {
		return rxHibernateSession.unwrap( EventSource.class ).getExceptionConverter();
	}

	private <T> Iterable<T> listeners(EventType<T> type) {
		return eventListenerGroup( type ).listeners();
	}

	private <T> EventListenerGroup<T> eventListenerGroup(EventType<T> type) {
		return factory.unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry().getService( EventListenerRegistry.class )
				.getEventListenerGroup( type );
	}

	@Override
	public CompletionStage<Void> remove(Object entity) {
		return CompletableFuture.runAsync( () -> {
			rxHibernateSession.remove( entity );
		} );
	}

	@Override
	public <R> RxQuery<R> createQuery(Class<R> resultType, String jpql) {
		return null;
	}

	@Override
	public StateControl sessionState() {
		return null;
	}

	private ServiceRegistry serviceRegistry() {
		return factory.unwrap( SessionFactoryImplementor.class ).getServiceRegistry();
	}
}
