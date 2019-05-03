package org.hibernate.rx.event;

import java.util.Map;

import org.hibernate.event.internal.DefaultPersistEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jpa.event.spi.CallbackRegistry;

public class DefaultRxPersistEventListener extends DefaultPersistEventListener {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxPersistEventListener.class );

	private CallbackRegistry callbackRegistry;

	public DefaultRxPersistEventListener() {
	}

	@Override
	protected void entityIsTransient(PersistEvent event, Map createCache) {
		LOG.trace( "Saving transient instance" );

		RxPersistEvent rxEvent = (RxPersistEvent) event;
		try {
			saveTransientEntity( rxEvent, createCache );
			rxEvent.complete();
		}
		catch (Throwable t) {
			rxEvent.completeExceptionally( t );
		}
	}

	private void saveTransientEntity(RxPersistEvent event, Map createCache) {
		final EventSource source = event.getSession();
		final Object entity = source.getPersistenceContext().unproxy( event.getObject() );
		if ( createCache.put( entity, entity ) == null ) {
			saveWithGeneratedId( entity, event.getEntityName(), createCache, source, false );
		}
	}

	public static class EventContextManagingPersistEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new EventContextManagingPersistEventListenerDuplicationStrategy();

		private EventContextManagingPersistEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof DefaultRxPersistEventListener && original instanceof PersistEventListener ) {
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
