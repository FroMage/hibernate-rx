/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostDeleteEventListener;
import org.hibernate.event.spi.PreDeleteEvent;
import org.hibernate.event.spi.PreDeleteEventListener;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;

public final class RxEntityDeleteAction extends EntityDeleteAction {
	private final CompletionStage<?> stage;
	private final boolean isCascadeDeleteEnabled;
	private final Object version;
	private final Object[] state;
	private final Object[] naturalIdValues;
	private SoftLock lock;

	public RxEntityDeleteAction(
			final Object id,
			final Object[] state,
			final Object version,
			final Object instance,
			final EntityTypeDescriptor entityDescriptor,
			final boolean isCascadeDeleteEnabled,
			final EventSource source,
			CompletionStage<?> stage) {
		super(id, state,version,instance,entityDescriptor, isCascadeDeleteEnabled, source);
		this.stage = stage;
		this.isCascadeDeleteEnabled = isCascadeDeleteEnabled;
		this.version = version;
		this.state = state;

		// before remove we need to remove the local (transactional) natural id cross-reference
		this.naturalIdValues = source.getPersistenceContext().getNaturalIdHelper().removeLocalNaturalIdCrossReference(
				getEntityDescriptor(),
				getId(),
				state
		);
	}

	@Override
	public void execute() throws HibernateException {
		final Object id = getId();
		final RxSingleTableEntityTypeDescriptor entityDescriptor = (RxSingleTableEntityTypeDescriptor) getEntityDescriptor();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();

		// TODO: This is not reactive right now
		final boolean veto = preDelete();

		Object version = this.version;
		if ( entityDescriptor.isVersionPropertyGenerated() ) {
			// we need to grab the version value from the entity, otherwise
			// we have issues with generated-version entities that may have
			// multiple actions queued during the same flush
			version = entityDescriptor.getVersion( instance );
		}

		final Object ck;
		if ( entityDescriptor.canWriteToCache() ) {
			final EntityDataAccess cache = entityDescriptor.getHierarchy().getEntityCacheAccess();
			ck = cache.generateCacheKey( id, entityDescriptor.getHierarchy(), session.getFactory(), session.getTenantIdentifier() );
			lock = cache.lockItem( session, ck, version );
		}
		else {
			ck = null;
		}

		if ( !isCascadeDeleteEnabled && !veto ) {
			entityDescriptor.delete( id, version, instance, session, stage );
		}

		// FIXME: need testing
		stage.thenAccept( (ignore) -> {
			//postDelete:
			// After actually deleting a row, record the fact that the instance no longer
			// exists on the database (needed for identity-column key generation), and
			// remove it from the session cache
			final PersistenceContext persistenceContext = session.getPersistenceContext();
			final EntityEntry entry = persistenceContext.removeEntry( instance );
			if ( entry == null ) {
				throw new AssertionFailure( "possible nonthreadsafe access to session" );
			}
			entry.postDelete();

			persistenceContext.removeEntity( entry.getEntityKey() );
			persistenceContext.removeProxy( entry.getEntityKey() );

			if ( entityDescriptor.canWriteToCache() ) {
				entityDescriptor.getHierarchy().getEntityCacheAccess().remove( session, ck );
			}

			persistenceContext.getNaturalIdHelper().removeSharedNaturalIdCrossReference(
					entityDescriptor,
					id,
					naturalIdValues
			);

			postDelete();

			if ( getSession().getFactory().getStatistics().isStatisticsEnabled() && !veto ) {
				getSession().getFactory().getStatistics().deleteEntity( getEntityDescriptor().getEntityName() );
			}
		} );
	}

	private void postDelete() {
		final EventListenerGroup<PostDeleteEventListener> listenerGroup = listenerGroup( EventType.POST_DELETE );
		if ( listenerGroup.isEmpty() ) {
			return;
		}
		final PostDeleteEvent event = new PostDeleteEvent(
				getInstance(),
				getId(),
				state,
				getEntityDescriptor(),
				eventSource()
		);
		for ( PostDeleteEventListener listener : listenerGroup.listeners() ) {
			listener.onPostDelete( event );
		}
	}

	private boolean preDelete() {
		boolean veto = false;
		final EventListenerGroup<PreDeleteEventListener> listenerGroup = listenerGroup( EventType.PRE_DELETE );
		if ( listenerGroup.isEmpty() ) {
			return veto;
		}
		final PreDeleteEvent event = new PreDeleteEvent( getInstance(), getId(), state, getEntityDescriptor(), eventSource() );
		for ( PreDeleteEventListener listener : listenerGroup.listeners() ) {
			veto |= listener.onPreDelete( event );
		}
		return veto;
	}
}
