package org.hibernate.rx.event;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityEntryExtraState;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SelfDirtinessTracker;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.DefaultPersistEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.metamodel.model.domain.spi.EntityIdentifier;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.metamodel.model.domain.spi.StateArrayContributor;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.impl.RxEntityInsertAction;
import org.hibernate.type.internal.TypeHelper;

import static org.hibernate.FlushMode.COMMIT;
import static org.hibernate.FlushMode.MANUAL;

public class DefaultRxPersistEventListener extends DefaultPersistEventListener implements RxPersistEventListener {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxPersistEventListener.class );

	private CallbackRegistry callbackRegistry;

	public DefaultRxPersistEventListener() {
	}

	@Override
	public void injectCallbackRegistry(CallbackRegistry callbackRegistry) {
		super.injectCallbackRegistry( callbackRegistry );
		this.callbackRegistry = callbackRegistry;
	}

	@Override
	public void onPersist(RxPersistEvent event) throws HibernateException {
		super.onPersist( (PersistEvent) event );
	}

	@Override
	public void onPersist(RxPersistEvent event, Map<?, ?> createdAlready) throws HibernateException {
		super.onPersist( (PersistEvent) event, createdAlready );
	}

	@Override
	public void onPersist(PersistEvent event) throws HibernateException {
		super.onPersist( event );
	}

	@Override
	protected void entityIsTransient(PersistEvent event, Map createCache) {
		if ( event instanceof RxPersistEvent ) {
			LOG.trace( "Saving transient instance" );

			RxPersistEvent rxEvent = (RxPersistEvent) event;
			try {
				saveTransientEntity( rxEvent, createCache );
			}
			catch (Throwable t) {
				rxEvent.completeExceptionally( t );
			}
		}
		else {
			super.entityIsTransient( event, createCache );
		}
	}

	private void saveTransientEntity(RxPersistEvent event, Map createCache ) {
		final EventSource source = event.getSession();
		final Object entity = source.getPersistenceContext().unproxy( event.getObject() );
		if ( createCache.put( entity, entity ) == null ) {
			saveWithGeneratedId( entity, event.getEntityName(), createCache, source,false )
					.exceptionally( err -> { event.completeExceptionally( err ); return null; } )
					.thenAccept( ignore -> { event.complete(); } );
		}
	}

	protected CompletionStage<Object> saveWithGeneratedId(
			Object entity,
			String entityName,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess) {
		CompletableFuture<Object> saveWithIdStage = new CompletableFuture<>();
		callbackRegistry.preCreate( entity );

		if ( entity instanceof SelfDirtinessTracker ) {
			( (SelfDirtinessTracker) entity ).$$_hibernate_clearDirtyAttributes();
		}

		final EntityTypeDescriptor entityDescriptor = source.getEntityDescriptor( entityName, entity );
		final EntityIdentifier<Object, Object> identifierDescriptor = entityDescriptor.getHierarchy()
				.getIdentifierDescriptor();
		Object generatedId = identifierDescriptor
				.getIdentifierValueGenerator()
				.generate( source, entity );
		if ( generatedId == null ) {
			saveWithIdStage.completeExceptionally( new IdentifierGenerationException( "null id generated for:" + entity.getClass() ) );
			return saveWithIdStage;
		}
		else if ( generatedId == IdentifierGeneratorHelper.SHORT_CIRCUIT_INDICATOR ) {
			saveWithIdStage.complete( source.getIdentifier( entity ) );
			return saveWithIdStage;
		}
		else if ( generatedId == IdentifierGeneratorHelper.POST_INSERT_INDICATOR ) {
			return performSave( entity, null, entityDescriptor, true, anything, source, requiresImmediateIdAccess, saveWithIdStage );
		}
		else {
			// TODO: define toString()s for generators
			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Generated identifier: %s, using strategy: %s",
						entityDescriptor.getIdentifierDescriptor().getJavaTypeDescriptor().extractLoggableRepresentation( generatedId ),
						identifierDescriptor.getClass().getName()
				);
			}

			return performSave( entity, generatedId, entityDescriptor, false, anything, source, true, saveWithIdStage );
		}
	}

	protected CompletionStage<Object> performSave(
			Object entity,
			Object id,
			EntityTypeDescriptor descriptor,
			boolean useIdentityColumn,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess,
			CompletionStage<Object> performSaveStage) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Saving {0}", MessageHelper.infoString( descriptor, id, source.getFactory() ) );
		}

		final EntityKey key;
		if ( !useIdentityColumn ) {
			key = source.generateEntityKey( id, descriptor );
			Object old = source.getPersistenceContext().getEntity( key );
			if ( old != null ) {
				if ( source.getPersistenceContext().getEntry( old ).getStatus() == Status.DELETED ) {
					source.forceFlush( source.getPersistenceContext().getEntry( old ) );
				}
				else {
					performSaveStage.toCompletableFuture().completeExceptionally( new NonUniqueObjectException( id, descriptor.getEntityName() ) );
					return performSaveStage;
				}
			}
			descriptor.setIdentifier( entity, id, source );
		}
		else {
			key = null;
		}

		if ( invokeSaveLifecycle( entity, descriptor, source ) ) {
			//EARLY EXIT
			performSaveStage.toCompletableFuture().complete( id );
			return performSaveStage;
		}

		return performSaveOrReplicate(
				entity,
				key,
				descriptor,
				useIdentityColumn,
				anything,
				source,
				requiresImmediateIdAccess,
				performSaveStage
		);
	}

	protected CompletionStage<Object> performSaveOrReplicate(
			Object entity,
			EntityKey key,
			EntityTypeDescriptor entityDescriptor,
			boolean useIdentityColumn,
			Object anything,
			EventSource source,
			boolean requiresImmediateIdAccess,
			CompletionStage stage) {

		Object id = key == null ? null : key.getIdentifier();

		boolean shouldDelayIdentityInserts = shouldDelayIdentityInserts( requiresImmediateIdAccess, source, entityDescriptor );

		// Put a placeholder in entries, so we don't recurse back and try to save() the
		// same object again. QUESTION: should this be done before onSave() is called?
		// likewise, should it be done before onUpdate()?

		// todo (6.0) : Should we do something here like `org.hibernate.sql.results.spi.JdbcValuesSourceProcessingState#registerLoadingEntity` ?
		EntityEntry original = source.getPersistenceContext().addEntry(
				entity,
				Status.SAVING,
				null,
				null,
				id,
				null,
				LockMode.WRITE,
				useIdentityColumn,
				entityDescriptor,
				false
		);

		cascadeBeforeSave( source, entityDescriptor, entity, anything );

		Object[] values = entityDescriptor.getPropertyValuesToInsert( entity, getMergeMap( anything ), source );
//		final List<Navigable> navigables = entityDescriptor.getNavigables();

		boolean substitute = substituteValuesIfNecessary( entity, id, values, entityDescriptor, source );

		if ( entityDescriptor.hasCollections() ) {
			substitute = substitute || visitCollectionsBeforeSave( entity, id, values, entityDescriptor.getPersistentAttributes(), source );
		}

		if ( substitute ) {
			entityDescriptor.setPropertyValues( entity, values );
		}

		TypeHelper.deepCopy(
				entityDescriptor,
				values,
				values,
				StateArrayContributor::isUpdatable
		);

		CompletionStage<Object> postInsertStage = stage.thenAccept( ignore -> {

			//			// postpone initializing id in case the insert has non-nullable transient dependencies
//			// that are not resolved until cascadeAfterSave() is executed
//			cascadeAfterSave( source, entityDescriptor, entity, anything );
//			if ( useIdentityColumn && insert.isEarlyInsert() ) {
//				if ( !EntityIdentityInsertAction.class.isInstance( insert ) ) {
//					throw new IllegalStateException(
//							"Insert should be using an identity column, but action is of unexpected type: " +
//									insert.getClass().getName()
//					);
//				}
//				id = ( (EntityIdentityInsertAction) insert ).getGeneratedId();
//
//				insert.handleNaturalIdPostSaveNotifications( id );
//			}

			EntityEntry newEntry = source.getPersistenceContext().getEntry( entity );

			if ( newEntry != original ) {
				EntityEntryExtraState extraState = newEntry.getExtraState( EntityEntryExtraState.class );
				if ( extraState == null ) {
					newEntry.addExtraState( original.getExtraState( EntityEntryExtraState.class ) );
				}
			}
		} );

		RxEntityInsertAction insert = createInsertAction(
				values,
				id,
				entity,
				entityDescriptor,
				useIdentityColumn,
				source,
				shouldDelayIdentityInserts,
				stage
		);

		( (RxSession) source ).getRxActionQueue().addAction( insert );
		return postInsertStage;
	}


	private static boolean shouldDelayIdentityInserts(
			boolean requiresImmediateIdAccess,
			EventSource source,
			EntityTypeDescriptor descriptor) {
		return shouldDelayIdentityInserts(
				requiresImmediateIdAccess,
				isPartOfTransaction( source ),
				source.getHibernateFlushMode(),
				descriptor
		);
	}

	private static boolean isPartOfTransaction(EventSource source) {
		return source.isTransactionInProgress() && source.getTransactionCoordinator().isJoined();
	}

	private static boolean shouldDelayIdentityInserts(
			boolean requiresImmediateIdAccess,
			boolean partOfTransaction,
			FlushMode flushMode,
			EntityTypeDescriptor entityDescriptor) {
		if ( !entityDescriptor.getFactory().getSessionFactoryOptions().isPostInsertIdentifierDelayableEnabled() ) {
			return false;
		}

		if ( requiresImmediateIdAccess ) {
			// todo : make this configurable?  as a way to support this behavior with Session#save etc
			return false;
		}

		// otherwise we should delay the IDENTITY insertions if either:
		//		1) we are not part of a transaction
		//		2) we are in FlushMode MANUAL or COMMIT (not AUTO nor ALWAYS)
		if ( !partOfTransaction || flushMode == MANUAL || flushMode == COMMIT ) {
			if ( entityDescriptor.canIdentityInsertBeDelayed() ) {
				return true;
			}
			/*LOG.debugf(
					"Identity insert for entity [%s] should be delayed; however the persister requested early insert.",
					entityDescriptor.getEntityName()
			);*/
			return false;
		}
		else {
			return false;
		}
	}

	private RxEntityInsertAction createInsertAction(
			Object[] values,
			Object id,
			Object entity,
			EntityTypeDescriptor descriptor,
			boolean useIdentityColumn,
			EventSource source,
			boolean shouldDelayIdentityInserts,
			CompletionStage<Object> stage) {

		//		AbstractEntityInsertAction insertAction = null;
//		if ( useIdentityColumn ) {
//			insertAction = new RxEntityIdentityInsertAction(
//					values,
//					entity,
//					descriptor,
//					isVersionIncrementDisabled(),
//					source,
//					shouldDelayIdentityInserts,
//					null // Stage
//			);
//		}
//		else {
			Object version = Versioning.getVersion( values, descriptor );
			RxEntityInsertAction insertAction = new RxEntityInsertAction(
					id,
					values,
					entity,
					version,
					descriptor,
					isVersionIncrementDisabled(),
					source,
					stage
			);
//		}
		return insertAction;

	}
	@Override
	public void onPersist(PersistEvent event, Map createdAlready) throws HibernateException {
		super.onPersist( event, createdAlready );
	}

	public static class EventContextManagingPersistEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new EventContextManagingPersistEventListenerDuplicationStrategy();

		private EventContextManagingPersistEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof RxPersistEventListener && original instanceof PersistEventListener ) {
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
