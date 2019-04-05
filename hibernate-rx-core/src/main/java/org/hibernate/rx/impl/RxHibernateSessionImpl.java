package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import javax.persistence.EntityTransaction;

import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.SessionDelegatorBaseImpl;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.transaction.spi.TransactionImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.internal.ExceptionMapperStandardImpl;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.resource.transaction.backend.jta.internal.synchronization.ExceptionMapper;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;
import org.hibernate.rx.event.RxPersistEvent;

public class RxHibernateSessionImpl extends SessionDelegatorBaseImpl implements RxHibernateSession, EventSource {

	private final CompletionStage<EntityTransaction> transactionStage = CompletableFuture.completedFuture( null );
	private final RxHibernateSessionFactoryImplementor factory;
	private final ExceptionConverter exceptionConverter;
	private final ExceptionMapper exceptionMapper = ExceptionMapperStandardImpl.INSTANCE;
	private transient RxActionQueue rxActionQueue;
	private RxSessionImpl rxSession;

	public RxHibernateSessionImpl(RxHibernateSessionFactoryImplementor factory, SessionImplementor delegate) {
		super( delegate );
		this.rxActionQueue = new RxActionQueue( this );
		this.factory = factory;
		this.exceptionConverter = delegate.getExceptionConverter();
	}

	@Override
	public TransactionCoordinator getTransactionCoordinator() {
		return super.getTransactionCoordinator();
	}

	@Override
	public RxHibernateSessionFactoryImplementor getSessionFactory() {
		return factory;
	}

	@Override
	public RxActionQueue getRxActionQueue() {
		return rxActionQueue;
	}

	@Override
	public EntityTypeDescriptor getEntityDescriptor(final String entityName, final Object object) {
//		checkOpenOrWaitingForAutoClose();
		if ( entityName == null ) {
			return getFactory().getMetamodel().getEntityDescriptor( guessEntityName( object ) );
		}
		else {
			// try block is a hack around fact that currently tuplizers are not
			// given the opportunity to resolve a subclass entity name.  this
			// allows the (we assume custom) interceptor the ability to
			// influence this decision if we were not able to based on the
			// given entityName
			try {
				EntityTypeDescriptor typeDescriptor = getFactory().getMetamodel()
						.findEntityDescriptor( entityName )
						.getSubclassEntityDescriptor( object, getFactory() );
				return typeDescriptor;
			}
			catch (HibernateException e) {
				try {
					return getEntityDescriptor( null, object );
				}
				catch (HibernateException e2) {
					throw e;
				}
			}
		}
	}

	@Override
	public SessionImplementor getSession() {
		return this;
	}

	@Override
	public CompletionStage<Void> persistAsync(Object object) {
		final CompletableFuture<Void> persistStage = new CompletableFuture<>();
		CompletableFuture.runAsync( () -> {
			checkOpen();
			firePersist( new RxPersistEvent( null, object, this, this.reactive(), persistStage ) );
			flush();
		} );
		return persistStage;
	}

	private void firePersist(PersistEvent event) {
		try {
//			checkTransactionSynchStatus();
//			checkNoUnresolvedActionsBeforeOperation();

			for ( PersistEventListener listener : listeners( EventType.PERSIST ) ) {
				listener.onPersist( event );
			}
		}
		catch (MappingException e) {
			throw exceptionConverter.convert( new IllegalArgumentException( e.getMessage() ) );
		}
		catch (RuntimeException e) {
			throw exceptionConverter.convert( e );
		}
		finally {
//			try {
//				checkNoUnresolvedActionsAfterOperation();
//			}
//			catch (RuntimeException e) {
//				throw exceptionConverter.convert( e );
//			}
		}
	}

	@Override
	public void flush() {
		checkOpen();
		doFlush();
	}

	private void doFlush() {
//		checkTransactionNeeded();
//		checkTransactionSynchStatus();

		try {
//			if ( persistenceContext.getCascadeLevel() > 0 ) {
//				throw new HibernateException( "Flush during cascade is dangerous" );
//			}

			FlushEvent flushEvent = new FlushEvent( this );
			for ( FlushEventListener listener : listeners( EventType.FLUSH ) ) {
				listener.onFlush( flushEvent );
			}

//			delayedAfterCompletion();
		}
		catch (RuntimeException e) {
			throw exceptionConverter.convert( e );
		}
	}

	private <T> Iterable<T> listeners(EventType<T> type) {
		return eventListenerGroup( type ).listeners();
	}

	private <T> EventListenerGroup<T> eventListenerGroup(EventType<T> type) {
		return getFactory().getServiceRegistry()
				.getService( EventListenerRegistry.class )
				.getEventListenerGroup( type );
	}

	private void managedFlush() {
		if ( isClosed() ) {// && !waitingForAutoClose ) {
//			log.trace( "Skipping auto-flush due to session closed" );
			return;
		}
//		log.trace( "Automatically flushing session" );
		doFlush();
	}

	@Override
	public void flushBeforeTransactionCompletion() {
		final boolean doFlush = isTransactionFlushable()
				&& getHibernateFlushMode() != FlushMode.MANUAL;

		try {
			if ( doFlush ) {
				managedFlush();
			}
		}
		catch (RuntimeException re) {
			throw exceptionMapper.mapManagedFlushFailure( "error during managed flush", re, this );
		}
	}

	private boolean isTransactionFlushable() {
		if ( getCurrentTransaction() == null ) {
			// assume it is flushable - CMT, auto-commit, etc
			return true;
		}
		final TransactionStatus status = getCurrentTransaction().getStatus();
		return status == TransactionStatus.ACTIVE || status == TransactionStatus.COMMITTING;
	}

	// SessionImpl
	protected TransactionImplementor getCurrentTransaction() {
		return (TransactionImplementor) getTransaction();
	}

	@Override
	public RxSession reactive() {
		if ( rxSession == null ) {
			rxSession = new RxSessionImpl( factory, this );
		}
		return rxSession;
	}

	@Override
	public void reactive(Consumer<RxSession> consumer) {
		consumer.accept( new RxSessionImpl( factory, this ) );
	}
}

