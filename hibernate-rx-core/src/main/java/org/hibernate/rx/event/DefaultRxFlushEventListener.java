package org.hibernate.rx.event;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.DefaultFlushEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.FlushEntityEvent;
import org.hibernate.event.spi.FlushEntityEventListener;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.EntityPrinter;
import org.hibernate.internal.util.collections.LazyIterator;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.engine.spi.RxActionQueue;

import org.jboss.logging.Logger;

public class DefaultRxFlushEventListener extends DefaultFlushEventListener {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			DefaultRxFlushEventListener.class.getName()
	);

	@Override
	protected void performExecutions(EventSource session) {
		LOG.trace( "Executing flush" );

		// IMPL NOTE : here we alter the flushing flag of the persistence context to allow
		//		during-flush callbacks more leniency in regards to initializing proxies and
		//		lazy collections during their processing.
		// For more information, see HHH-2763
		try {
			session.getJdbcCoordinator().flushBeginning();
			session.getPersistenceContext().setFlushing( true );
			// we need to lock the collection caches before executing entity inserts/updates in order to
			// account for bi-directional associations
			actionQueue( session ).prepareActions();
			actionQueue( session ).executeActions();
		}
		finally {
			session.getPersistenceContext().setFlushing( false );
			session.getJdbcCoordinator().flushEnding();
		}
	}

	/**
	 * 1. detect any dirty entities
	 * 2. schedule any entity updates
	 * 3. search out any reachable collections
	 */
	private int flushEntities(final FlushEvent event, final PersistenceContext persistenceContext)
			throws HibernateException {

		LOG.trace( "Flushing entities and processing referenced collections" );

		final EventSource source = event.getSession();
		final Iterable<FlushEntityEventListener> flushListeners = source.getFactory().getServiceRegistry()
				.getService( EventListenerRegistry.class )
				.getEventListenerGroup( EventType.FLUSH_ENTITY )
				.listeners();

		// Among other things, updateReachables() will recursively load all
		// collections that are moving roles. This might cause entities to
		// be loaded.

		// So this needs to be safe from concurrent modification problems.

		final Map.Entry<Object, EntityEntry>[] entityEntries = persistenceContext.reentrantSafeEntityEntries();
		final int count = entityEntries.length;

		for ( Map.Entry<Object, EntityEntry> me : entityEntries ) {

			// Update the status of the object and if necessary, schedule an update

			EntityEntry entry = me.getValue();
			Status status = entry.getStatus();

			// FIXME: I'm pretty sure we need a different completion stage but this should work for now
			//        I will fix it when I'm going to work on the case for when we have more than one liste
			CompletionStage<Void> flushStage = ( (RxFlushEvent) event ).getFlushStage();
			if ( status != Status.LOADING && status != Status.GONE ) {
				final FlushEntityEvent entityEvent = new RxFlushEntityEvent( source, me.getKey(), entry, flushStage );
				for ( FlushEntityEventListener listener : flushListeners ) {
					listener.onFlushEntity( entityEvent );
				}
			}
		}

		source.unwrap( RxHibernateSession.class ).getRxActionQueue().sortActions();

		return count;
	}

	@SuppressWarnings(value = { "unchecked" })
	private void logFlushResults(FlushEvent event) {
		if ( !LOG.isDebugEnabled() ) {
			return;
		}
		final EventSource session = event.getSession();
		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		RxActionQueue actionQueue = actionQueue( session );
		LOG.debugf(
				"Flushed: %s insertions, %s updates, %s deletions to %s objects",
				actionQueue.numberOfInsertions(),
				actionQueue.numberOfUpdates(),
				actionQueue.numberOfDeletions(),
				persistenceContext.getNumberOfManagedEntities()
		);
		LOG.debugf(
				"Flushed: %s (re)creations, %s updates, %s removals to %s collections",
				actionQueue.numberOfCollectionCreations(),
				actionQueue.numberOfCollectionUpdates(),
				actionQueue.numberOfCollectionRemovals(),
				persistenceContext.getCollectionEntries().size()
		);
		new EntityPrinter( session.getFactory() ).toString(
				persistenceContext.getEntitiesByKey().entrySet()
		);
	}

	/**
	 * Coordinates the processing necessary to get things ready for executions
	 * as db calls by preping the session caches and moving the appropriate
	 * entities and collections to their respective execution queues.
	 *
	 * @param event The flush event.
	 *
	 * @throws HibernateException Error flushing caches to execution queues.
	 */
	protected void flushEverythingToExecutions(FlushEvent event) throws HibernateException {

		LOG.trace( "Flushing session" );

		EventSource session = event.getSession();

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		session.getInterceptor().preFlush( new LazyIterator( persistenceContext.getEntitiesByKey() ) );

//		prepareEntityFlushes( session, persistenceContext );
		// we could move this inside if we wanted to
		// tolerate collection initializations during
		// collection dirty checking:
//		prepareCollectionFlushes( persistenceContext );
		// now, any collections that are initialized
		// inside this block do not get updated - they
		// are ignored until the next flush

		persistenceContext.setFlushing( true );
		try {
			int entityCount = flushEntities( event, persistenceContext );
//			int collectionCount = flushCollections( session, persistenceContext );

			event.setNumberOfEntitiesProcessed( entityCount );
//			event.setNumberOfCollectionsProcessed( collectionCount );
		}
		finally {
			persistenceContext.setFlushing( false );
		}

		//some statistics
		logFlushResults( event );
	}

	private RxActionQueue actionQueue(EventSource session) {
		return session.unwrap( RxHibernateSession.class ).getRxActionQueue();
	}

	public static class EventContextManagingFlushEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new DefaultRxFlushEventListener.EventContextManagingFlushEventListenerDuplicationStrategy();

		private EventContextManagingFlushEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof DefaultRxFlushEventListener && original instanceof FlushEventListener ) {
				return true;
			}

			return false;
		}

		@Override
		public Action getAction() {
			return Action.REPLACE_ORIGINAL;
		}
	}
}
