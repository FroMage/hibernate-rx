package org.hibernate.rx.event;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.DelayedPostInsertIdentifier;
import org.hibernate.event.internal.DefaultFlushEventListener;
import org.hibernate.event.internal.DefaultLoadEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.impl.RxSingleTableEntityTypeDescriptor;

import org.jboss.logging.Logger;

public class DefaultRxLoadEventListener extends DefaultLoadEventListener {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger( CoreMessageLogger.class, DefaultRxLoadEventListener.class.getName() );

	/**
	 * Performs the process of loading an entity from the configured
	 * underlying datasource.
	 *
	 * @param event The load event
	 * @param entityDescriptor The entityDescriptor for the entity being requested for load
	 *
	 * @return The object loaded from the datasource, or null if not found.
	 */
	@Override
	protected Object loadFromDatasource(
			final LoadEvent event,
			final EntityTypeDescriptor entityDescriptor) {
		RxSingleTableEntityTypeDescriptor rxDescriptor = (RxSingleTableEntityTypeDescriptor) entityDescriptor;
		Object entity = entityDescriptor.getSingleIdLoader()
				.load( event.getEntityId(), event.getLockOptions(), event.getSession() );

		if ( event.isAssociationFetch() && event.getSession().getFactory().getStatistics().isStatisticsEnabled() ) {
			event.getSession().getFactory().getStatistics().fetchEntity( event.getEntityClassName() );
		}

		return entity;
	}

	public static class EventContextManagingLoadEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new DefaultRxLoadEventListener.EventContextManagingLoadEventListenerDuplicationStrategy();

		private EventContextManagingLoadEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof DefaultRxLoadEventListener && original instanceof LoadEventListener ) {
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
