package org.hibernate.rx.event;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.TransientObjectException;
import org.hibernate.engine.internal.ForeignKeys;
import org.hibernate.engine.internal.Nullability;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.DefaultDeleteEventListener;
import org.hibernate.event.internal.RxOnUpdateVisitor;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.metamodel.model.domain.spi.NonIdPersistentAttribute;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.impl.RxEntityDeleteAction;
import org.hibernate.type.internal.TypeHelper;

public class DefaultRxDeleteEventListener extends DefaultDeleteEventListener {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxDeleteEventListener.class );

	private CallbackRegistry callbackRegistry;

	public DefaultRxDeleteEventListener() {
	}

	@Override
	public void onDelete(DeleteEvent event, Set transientEntities) throws HibernateException {
		doDelete( event, transientEntities ).whenComplete( (ignore, err) -> {
			if ( err != null ) {
				( (RxDeleteEvent) event ).completeExceptionally( err );
			}
			else {
				( (RxDeleteEvent) event ).complete();
			}
		} );
	}

	private CompletionStage<?> doDelete(DeleteEvent event, Set transientEntities) {
		RxDeleteEvent rxEvent = (RxDeleteEvent) event;
		CompletionStage<Void> deleteStage = new CompletableFuture<>();
		EventSource source = event.getSession();
		final RxSession rxSession = source.unwrap( RxSession.class );

		final PersistenceContext persistenceContext = source.getPersistenceContext();
		Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );

		EntityEntry entityEntry = persistenceContext.getEntry( entity );
		final EntityTypeDescriptor descriptor;
		final Object id;
		final Object version;

		if ( entityEntry == null ) {
			LOG.trace( "Entity was not persistent in delete processing" );

			descriptor = source.getEntityDescriptor( event.getEntityName(), entity );

			if ( ForeignKeys.isTransient( descriptor.getEntityName(), entity, null, source.unwrap( SharedSessionContractImplementor.class ) ) ) {
				deleteTransientEntity( source.unwrap( EventSource.class ), entity, event.isCascadeDeleteEnabled(), descriptor, transientEntities );
				deleteStage.toCompletableFuture().complete( null );
				// EARLY EXIT!!!
				return deleteStage;
			}
			performDetachedEntityDeletionCheck( event );

			id = descriptor.getIdentifier( entity );

			if ( id == null ) {
				deleteStage.toCompletableFuture().completeExceptionally( new TransientObjectException(
						"the detached instance passed to delete() had a null identifier"
				) );
				return deleteStage;
			}

			final EntityKey key = source.generateEntityKey( id, descriptor );
			persistenceContext.checkUniqueness( key, entity );

			new RxOnUpdateVisitor( source, id, entity ).process( entity, descriptor );

			version = descriptor.getVersion( entity );

			entityEntry = persistenceContext.addEntity(
					entity,
					( descriptor.getHierarchy().getMutabilityPlan().isMutable() ? Status.MANAGED : Status.READ_ONLY ),
					descriptor.getPropertyValues( entity ),
					key,
					version,
					LockMode.NONE,
					true,
					descriptor,
					false
			);
		}
		else {
			LOG.trace( "Deleting a persistent instance" );

			if ( entityEntry.getStatus() == Status.DELETED || entityEntry.getStatus() == Status.GONE ) {
				LOG.trace( "Object was already deleted" );
				deleteStage.toCompletableFuture().complete( null );
				return deleteStage;
			}
			descriptor = entityEntry.getDescriptor();
			id = entityEntry.getId();
			version = entityEntry.getVersion();
		}

		/*if ( !descriptor.isMutable() ) {
			throw new HibernateException(
					"attempted to delete an object of immutable class: " +
					MessageHelper.infoString(descriptor)
				);
		}*/

		if ( invokeDeleteLifecycle( source, entity, descriptor ) ) {
			deleteStage.toCompletableFuture().complete( null );
			return deleteStage;
		}

		deleteStage.thenAccept( ignore -> {
			if ( source.getFactory().getSettings().isIdentifierRollbackEnabled() ) {
				descriptor.resetIdentifier( entity, id, version, source );
			}
		} );

		deleteEntity(
				source,
				entity,
				entityEntry,
				event.isCascadeDeleteEnabled(),
				event.isOrphanRemovalBeforeUpdates(),
				descriptor,
				transientEntities,
				deleteStage
		);

		return deleteStage;
	}

	protected final void deleteEntity(
			final EventSource source,
			final Object entity,
			final EntityEntry entityEntry,
			final boolean isCascadeDeleteEnabled,
			final boolean isOrphanRemovalBeforeUpdates,
			final EntityTypeDescriptor entityDescriptor,
			final Set transientEntities,
			final CompletionStage<Void> stage) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Deleting {0}",
					MessageHelper.infoString( entityDescriptor, entityEntry.getId(), source.getFactory() )
			);
		}

		final PersistenceContext persistenceContext = source.getPersistenceContext();
		final Object version = entityEntry.getVersion();

		final Object[] currentState;
		if ( entityEntry.getLoadedState() == null ) {
			//ie. the entity came in from update()
			currentState = entityDescriptor.getPropertyValues( entity );
		}
		else {
			currentState = entityEntry.getLoadedState();
		}

		final Object[] deletedState = createDeletedState( entityDescriptor, currentState, source );
		entityEntry.setDeletedState( deletedState );

		source.getInterceptor().onDelete(
				entity,
				entityEntry.getId(),
				deletedState,
				entityDescriptor.getPropertyNames(),
				entityDescriptor.getPropertyJavaTypeDescriptors()
		);

		// before any callbacks, etc, so subdeletions see that this deletion happened first
		persistenceContext.setEntryStatus( entityEntry, Status.DELETED );
		final EntityKey key = source.generateEntityKey( entityEntry.getId(), entityDescriptor );

		cascadeBeforeDelete( source, entityDescriptor, entity, entityEntry, transientEntities );

		final List<NonIdPersistentAttribute<?, ?>> attributes = entityDescriptor.getPersistentAttributes();
		new ForeignKeys.Nullifier( entity, true, false, source )
				.nullifyTransientReferences( entityEntry.getDeletedState(), attributes );
		new Nullability( source ).checkNullability( entityEntry.getDeletedState(), entityDescriptor, Nullability.NullabilityCheckType.DELETE );
		persistenceContext.getNullifiableEntityKeys().add( key );
//
//		if ( isOrphanRemovalBeforeUpdates ) {
//			// TODO: The removeOrphan concept is a temporary "hack" for HHH-6484.  This should be removed once action/task
//			// ordering is improved.
//			session.getRxActionQueue().addAction(
//					new OrphanRemovalAction(
//							entityEntry.getId(),
//							deletedState,
//							version,
//							entity,
//							entityDescriptor,
//							isCascadeDeleteEnabled,
//							session
//					)
//			);
//		}
//		else {
			// Ensures that containing deletions happen before sub-deletions
		CompletionStage<Void> withCascadeStage = stage.thenAccept( (ignore) -> {
			cascadeAfterDelete( source, entityDescriptor, entity, transientEntities );
		} );

		source.getActionQueue().addAction(
				new RxEntityDeleteAction(
						entityEntry.getId(),
						deletedState,
						version,
						entity,
						entityDescriptor,
						isCascadeDeleteEnabled,
						source,
						stage
				) );
//		}
//

		// the entry will be removed after the flush, and will no longer
		// override the stale snapshot
		// This is now handled by removeEntity() in EntityDeleteAction
		//persistenceContext.removeDatabaseSnapshot(key);
	}

	@SuppressWarnings("unchecked")
	private Object[] createDeletedState(EntityTypeDescriptor entityDescriptor, Object[] currentState, EventSource session) {
		final Object[] deletedState = new Object[ currentState.length ];

		TypeHelper.deepCopy(
				entityDescriptor,
				currentState,
				deletedState,
				(navigable) -> true
		);

		return deletedState;
	}


	public static class EventContextManagingDeleteEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new EventContextManagingDeleteEventListenerDuplicationStrategy();

		private EventContextManagingDeleteEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof DefaultDeleteEventListener && original instanceof DeleteEventListener ) {
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
