package org.hibernate.rx.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.persistence.CacheRetrieveMode;
import javax.persistence.CacheStoreMode;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.EntityTransaction;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceException;
import javax.persistence.TransactionRequiredException;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.IdentifierLoadAccess;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.ObjectDeletedException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Session;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.internal.StatefulPersistenceContext;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionDelegatorBaseImpl;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.transaction.spi.TransactionImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.EntityManagerMessageLogger;
import org.hibernate.internal.ExceptionMapperStandardImpl;
import org.hibernate.internal.HEMLogging;
import org.hibernate.jpa.internal.util.CacheModeHelper;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorImpl;
import org.hibernate.resource.transaction.backend.jta.internal.synchronization.ExceptionMapper;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;
import org.hibernate.rx.event.RxDeleteEvent;
import org.hibernate.rx.event.RxLoadEvent;
import org.hibernate.rx.event.RxPersistEvent;

import static org.hibernate.cfg.AvailableSettings.JPA_SHARED_CACHE_RETRIEVE_MODE;
import static org.hibernate.cfg.AvailableSettings.JPA_SHARED_CACHE_STORE_MODE;

public class RxSessionImpl extends SessionDelegatorBaseImpl implements RxSession, EventSource {
	private static final EntityManagerMessageLogger log = HEMLogging.messageLogger( RxSessionImpl.class );

	private final Executor executor;
	private final CompletionStage<EntityTransaction> transactionStage = CompletableFuture.completedFuture( null );
	private final RxHibernateSessionFactoryImplementor factory;
	private final ExceptionConverter exceptionConverter;
	private final ExceptionMapper exceptionMapper = ExceptionMapperStandardImpl.INSTANCE;
	private transient StatefulPersistenceContext persistenceContext;
	private transient boolean disallowOutOfTransactionUpdateOperations;


	public RxSessionImpl(RxHibernateSessionFactoryImplementor factory, SessionImplementor delegate) {
		super( delegate );
		this.disallowOutOfTransactionUpdateOperations = !factory.getSessionFactoryOptions()
				.isAllowOutOfTransactionUpdateOperations();
		this.factory = factory;
		this.exceptionConverter = delegate.getExceptionConverter();
		this.executor = ForkJoinPool.commonPool();
		this.persistenceContext = new StatefulPersistenceContext( this );
	}

	@Override
	public Executor getExecutor() {
		return executor;
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		checkOpen();

		if ( Session.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}
		if ( SessionImplementor.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}
		if ( SharedSessionContractImplementor.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}
		if ( EntityManager.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}

		throw new PersistenceException( "Hibernate cannot unwrap " + clazz );
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
	public <R> RxQuery<R> createQuery(Class<R> resultType, String jpql) {
		return null;
	}

	@Override
	public CompletionStage<Void> inTransaction(final Function<EntityTransaction, CompletionStage<Void>> consumer) {
		CompletableFuture txStage = new CompletableFuture();
		executor.execute( () -> {
			try {
				final EntityTransaction tx = beginTransaction();
				try {
					consumer.apply( tx )
							.exceptionally(  err -> { txStage.completeExceptionally( err ); return null; } )
							.thenAccept( ignore -> {
								tx.commit();
								txStage.complete( null );
							} );
				}
				catch (Throwable t) {
					txStage.completeExceptionally( t );
					tx.rollback();
				}
			}
			catch (Throwable t) {
				txStage.completeExceptionally( t );
			}
		} );
		return txStage;
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
		executor.execute( () -> {
			checkOpen();
			firePersist( new RxPersistEvent( null, object, this, persistStage ) );
			flush();
		} );
		return persistStage;
	}

	@Override
	public CompletionStage<Void> removeAsync(Object object) {
		return deleteAsync( object )
				.exceptionally( e -> {
					if ( e instanceof MappingException ) {
						throw exceptionConverter.convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					throw exceptionConverter.convert( (RuntimeException) e );
				} );
	}

	@Override
	public void flush() throws HibernateException {
		checkOpen();
		doFlush();
	}

	private void doFlush() {
		checkTransactionNeeded();
		checkTransactionSynchStatus();

		try {
			if ( getPersistenceContext().getCascadeLevel() > 0 ) {
				throw new HibernateException( "Flush during cascade is dangerous" );
			}

			FlushEvent flushEvent = new FlushEvent( this );
			for ( FlushEventListener listener : listeners( EventType.FLUSH ) ) {
				listener.onFlush( flushEvent );
			}

			delayedAfterCompletion();
		}
		catch ( RuntimeException e ) {
			throw exceptionConverter.convert( e );
		}
	}

	@Override
	public void clear() {
		getPersistenceContext().clear();
	}

	@Override
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
	}

	public CompletionStage<Void> deleteAsync(String entityName, Object object) throws HibernateException {
		final CompletableFuture<Void> deleteStage = new CompletableFuture<>();
		executor.execute( () -> {
			checkOpen();
			fireDelete( new RxDeleteEvent( entityName, object, this, deleteStage ) );
			// At the moment we don't support tx;
			flush();
		} );
		return deleteStage;
	}

	public CompletionStage<Void> deleteAsync(Object object) {
		final CompletableFuture<Void> deleteStage = new CompletableFuture<>();
		executor.execute( () -> {
			checkOpen();
			fireDelete( new RxDeleteEvent( object, this, deleteStage ) );
		} );
		return deleteStage;
	}

	private void fireDelete(DeleteEvent event) {
		try {
//			checkTransactionSynchStatus();
			for ( DeleteEventListener listener : listeners( EventType.DELETE ) ) {
				listener.onDelete( event );
			}
		}
		catch (ObjectDeletedException sse) {
			throw exceptionConverter.convert( new IllegalArgumentException( sse ) );
		}
		catch (MappingException e) {
			throw exceptionConverter.convert( new IllegalArgumentException( e.getMessage(), e ) );
		}
		catch (RuntimeException e) {
			//including HibernateException
			throw exceptionConverter.convert( e );
		}
		finally {
//			delayedAfterCompletion();
		}
	}

	public <T> AsyncIdentifierLoadAccess byId(Class<T> entityClass, CompletionStage<Optional<T>> findStage) {
		return new AsyncIdentifierLoadAccess( this, entityClass, findStage );
	}

	private <T> EntityTypeDescriptor<? extends T> locateEntityDescriptor(Class<T> entityClass) {
		return getFactory().getMetamodel().getEntityDescriptor( entityClass );
	}

	private <T> EntityTypeDescriptor<? extends T> locateEntityDescriptor(String entityName) {
		return getFactory().getMetamodel().findEntityDescriptor( entityName );
	}

	public void afterOperation(boolean success) {
		if ( !isTransactionInProgress() ) {
			getJdbcCoordinator().afterTransaction();
		}
	}

	private void checkNoUnresolvedActionsAfterOperation() {
		if ( getPersistenceContext().getCascadeLevel() == 0 ) {
			getActionQueue().checkNoUnresolvedActionsAfterOperation();
		}
		delayedAfterCompletion();
	}

	protected void checkOpenOrWaitingForAutoClose() {
//		if ( !waitingForAutoClose ) {
			checkOpen();
//		}
	}

	private void fireLoad(LoadEvent event, LoadEventListener.LoadType loadType) {
		checkOpenOrWaitingForAutoClose();
		checkTransactionSynchStatus();
		for ( LoadEventListener listener : listeners( EventType.LOAD ) ) {
			listener.onLoad( event, loadType );
		}
		delayedAfterCompletion();
	}

	@Override
	public <T> CompletionStage<Optional<T>> findAsync(Class<T> entityClass, Object primaryKey) {
		final CompletableFuture<Optional<T>> findStage = new CompletableFuture<>();
		CompletableFuture.runAsync( () -> {
			checkOpen();
			findAsync( entityClass, primaryKey, null, getProperties(), findStage );
		} );
		return findStage;
	}

	protected <T> void findAsync(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties,
			CompletionStage<Optional<T>> findStage) {
		checkOpen();
		LockOptions lockOptions = null;
		try {
			getLoadQueryInfluencers().getEffectiveEntityGraph().applyConfiguredGraph( properties );

			final AsyncIdentifierLoadAccess<T> loadAccess = byId( entityClass, findStage );
			loadAccess.with( determineAppropriateLocalCacheMode( properties ) );

			if ( lockModeType != null ) {
				if ( !LockModeType.NONE.equals( lockModeType ) ) {
					checkTransactionNeeded();
				}
				lockOptions = buildLockOptions( lockModeType, properties );
				loadAccess.with( lockOptions );
			}

			loadAccess.load( primaryKey );
		}
		catch (EntityNotFoundException ignored) {
			// DefaultLoadEventListener.returnNarrowedProxy may throw ENFE (see HHH-7861 for details),
			// which find() should not throw.  Find() should return null if the entity was not found.
			if ( log.isDebugEnabled() ) {
				String entityName = entityClass != null ? entityClass.getName() : null;
				String identifierValue = primaryKey != null ? primaryKey.toString() : null;
				log.ignoringEntityNotFound( entityName, identifierValue );
			}
//			return null;
		}
		catch (ObjectDeletedException e) {
			//the spec is silent about people doing remove() find() on the same PC
//			return null;
		}
		catch (ObjectNotFoundException e) {
			//should not happen on the entity itself with get
			throw new IllegalArgumentException( e.getMessage(), e );
		}
		catch (MappingException | TypeMismatchException | ClassCastException e) {
			throw exceptionConverter.convert( new IllegalArgumentException( e.getMessage(), e ) );
		}
		catch (JDBCException e) {
			if ( accessTransaction().getRollbackOnly() ) {
				// assume this is the similar to the WildFly / IronJacamar "feature" described under HHH-12472
//				return null;
			}
			else {
				throw exceptionConverter.convert( e, lockOptions );
			}
		}
		catch (RuntimeException e) {
			throw exceptionConverter.convert( e, lockOptions );
		}
		finally {
			getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
		}
	}

	private CacheMode determineAppropriateLocalCacheMode(Map<String, Object> localProperties) {
		CacheRetrieveMode retrieveMode = null;
		CacheStoreMode storeMode = null;
		if ( localProperties != null ) {
			retrieveMode = determineCacheRetrieveMode( localProperties );
			storeMode = determineCacheStoreMode( localProperties );
		}
		if ( retrieveMode == null ) {
			// use the EM setting
			retrieveMode = determineCacheRetrieveMode( getProperties() );
		}
		if ( storeMode == null ) {
			// use the EM setting
			storeMode = determineCacheStoreMode( getProperties() );
		}
		return CacheModeHelper.interpretCacheMode( storeMode, retrieveMode );
	}

	private CacheRetrieveMode determineCacheRetrieveMode(Map<String, Object> settings) {
		return (CacheRetrieveMode) settings.get( JPA_SHARED_CACHE_RETRIEVE_MODE );
	}

	private CacheStoreMode determineCacheStoreMode(Map<String, Object> settings) {
		return (CacheStoreMode) settings.get( JPA_SHARED_CACHE_STORE_MODE );
	}

	private void checkTransactionNeeded() {
		if ( disallowOutOfTransactionUpdateOperations && !isTransactionInProgress() ) {
			throw new TransactionRequiredException( "no transaction is in progress" );
		}
	}

	private void firePersist(PersistEvent event) {
		try {
			checkTransactionSynchStatus();
			checkNoUnresolvedActionsBeforeOperation();

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
			try {
				checkNoUnresolvedActionsAfterOperation();
			}
			catch (RuntimeException e) {
				throw exceptionConverter.convert( e );
			}
		}
	}

	protected void checkTransactionSynchStatus() {
		pulseTransactionCoordinator();
		delayedAfterCompletion();
	}

	protected void pulseTransactionCoordinator() {
		if ( !isClosed() ) {
			getTransactionCoordinator().pulse();
		}
	}

	protected void delayedAfterCompletion() {
		if ( getTransactionCoordinator() instanceof JtaTransactionCoordinatorImpl ) {
			( (JtaTransactionCoordinatorImpl) getTransactionCoordinator() ).getSynchronizationCallbackCoordinator()
					.processAnyDelayedAfterCompletion();
		}
	}

	private void checkNoUnresolvedActionsBeforeOperation() {
		if ( getPersistenceContext().getCascadeLevel() == 0 && getActionQueue().hasUnresolvedEntityInsertActions() ) {
			throw new IllegalStateException( "There are delayed insert actions before operation as cascade level 0." );
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
		if ( isClosed() ) {//wa && !waitingForAutoClose ) {
			log.trace( "Skipping auto-flush due to session closed" );
			return;
		}
		log.trace( "Automatically flushing session" );
		flush();
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

	private class AsyncIdentifierLoadAccess<T> implements IdentifierLoadAccess<T> {
		private final EntityTypeDescriptor entityDescriptor;
		private final CompletionStage<Optional<T>> findStage;
		private final RxSession session;

		private LockOptions lockOptions;
		private CacheMode cacheMode;
		private RootGraphImplementor<T> rootGraph;
		private GraphSemantic graphSemantic;

		private AsyncIdentifierLoadAccess(
				RxSession session,
				EntityTypeDescriptor entityDescriptor,
				CompletionStage<Optional<T>> findStage) {
			this.entityDescriptor = entityDescriptor;
			this.findStage = findStage;
			this.session = session;
		}

		private AsyncIdentifierLoadAccess(
				RxSession session,
				String entityName,
				CompletionStage<Optional<T>> findStage) {
			this( session, locateEntityDescriptor( entityName ), findStage );
		}

		private AsyncIdentifierLoadAccess(
				RxSession session,
				Class<T> entityClass,
				CompletionStage<Optional<T>> findStage) {
			this( session, locateEntityDescriptor( entityClass ), findStage );
		}

		@Override
		public final AsyncIdentifierLoadAccess<T> with(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		@Override
		public IdentifierLoadAccess<T> with(CacheMode cacheMode) {
			this.cacheMode = cacheMode;
			return this;
		}

		@Override
		public IdentifierLoadAccess<T> with(RootGraph<T> graph, GraphSemantic semantic) {
			this.rootGraph = (RootGraphImplementor<T>) graph;
			this.graphSemantic = semantic;
			return this;
		}

		@Override
		public final T getReference(Object id) {
			return perform( () -> doGetReference( id ) );
		}

		protected T perform(Supplier<T> executor) {
			CacheMode sessionCacheMode = getCacheMode();
			boolean cacheModeChanged = false;
			if ( cacheMode != null ) {
				// naive check for now...
				// todo : account for "conceptually equal"
				if ( cacheMode != sessionCacheMode ) {
					setCacheMode( cacheMode );
					cacheModeChanged = true;
				}
			}

			try {
				if ( graphSemantic != null ) {
					if ( rootGraph != null ) {
						throw new IllegalArgumentException( "Graph semantic specified, but no RootGraph was supplied" );
					}
					getLoadQueryInfluencers().getEffectiveEntityGraph().applyGraph( rootGraph, graphSemantic );
				}

				try {
					return executor.get();
				}
				finally {
					if ( graphSemantic != null ) {
						getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
					}
				}
			}
			finally {
				if ( cacheModeChanged ) {
					// change it back
					setCacheMode( sessionCacheMode );
				}
			}
		}

		@SuppressWarnings("unchecked")
		protected T doGetReference(Object id) {
			if ( this.lockOptions != null ) {
				RxLoadEvent event = new RxLoadEvent(
						id,
						entityDescriptor.getEntityName(),
						lockOptions,
						session,
						findStage
				);
				fireLoad( event, LoadEventListener.LOAD );
				return (T) event.getResult();
			}

			RxLoadEvent event = new RxLoadEvent(
					id,
					entityDescriptor.getEntityName(),
					false,
					session,
					findStage
			);
			boolean success = false;
			try {
				fireLoad( event, LoadEventListener.LOAD );
				if ( event.getResult() == null ) {
					getFactory().getEntityNotFoundDelegate().handleEntityNotFound(
							entityDescriptor.getEntityName(),
							id
					);
				}
				success = true;
				return (T) event.getResult();
			}
			finally {
				afterOperation( success );
			}
		}

		@Override
		public final T load(Object id) {
			return perform( () -> doLoad( id ) );
		}

		@Override
		public Optional<T> loadOptional(Serializable id) {
			return Optional.ofNullable( perform( () -> doLoad( id ) ) );
		}

		@SuppressWarnings("unchecked")
		protected final T doLoad(Object id) {
			if ( this.lockOptions != null ) {
				RxLoadEvent event = new RxLoadEvent(
						id,
						entityDescriptor.getEntityName(),
						lockOptions,
						session,
						findStage
				);
				fireLoad( event, LoadEventListener.GET );
				return (T) event.getResult();
			}

			RxLoadEvent event = new RxLoadEvent(
					id,
					entityDescriptor.getEntityName(),
					false,
					session,
					findStage
			);
			boolean success = false;
			try {
				fireLoad( event, LoadEventListener.GET );
				success = true;
			}
			catch (ObjectNotFoundException e) {
				// if session cache contains proxy for non-existing object
			}
			finally {
				afterOperation( success );
			}
			return (T) event.getResult();
		}

	}
}

