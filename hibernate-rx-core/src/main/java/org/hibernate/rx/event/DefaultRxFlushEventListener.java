package org.hibernate.rx.event;

import org.hibernate.HibernateException;
import org.hibernate.event.internal.DefaultFlushEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.rx.RxHibernateSession;

import org.jboss.logging.Logger;

public class DefaultRxFlushEventListener extends DefaultFlushEventListener {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger( CoreMessageLogger.class, DefaultRxFlushEventListener.class.getName() );

	@Override
	public void onFlush(FlushEvent event) throws HibernateException {
		super.onFlush( event );
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
