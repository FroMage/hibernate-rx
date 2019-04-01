package org.hibernate.rx.event;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.event.internal.AbstractFlushingEventListener;
import org.hibernate.event.internal.DefaultFlushEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.rx.RxHibernateSession;

import org.jboss.logging.Logger;

public class RxFlushEventListener extends DefaultFlushEventListener {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger( CoreMessageLogger.class, RxFlushEventListener.class.getName() );

	@Override
	public void onFlush(FlushEvent event) throws HibernateException {

		final EventSource source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContext();

		if ( persistenceContext.getNumberOfManagedEntities() > 0 ||
				persistenceContext.getCollectionEntries().size() > 0 ) {

			try {
				source.getEventListenerManager().flushStart();

				flushEverythingToExecutions( event );
				performExecutions( source );
				postFlush( source );
			}
			finally {
				source.getEventListenerManager().flushEnd(
						event.getNumberOfEntitiesProcessed(),
						event.getNumberOfCollectionsProcessed()
				);
			}

			postPostFlush( source );

			if ( source.getFactory().getStatistics().isStatisticsEnabled() ) {
				source.getFactory().getStatistics().flush();
			}
		}
	}

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
			session.getActionQueue().prepareActions();
			( (RxHibernateSession) session ).getRxActionQueue().executeActions();
		}
		finally {
			session.getPersistenceContext().setFlushing( false );
			session.getJdbcCoordinator().flushEnding();
		}
	}

}
