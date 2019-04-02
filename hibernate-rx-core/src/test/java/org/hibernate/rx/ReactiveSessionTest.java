package org.hibernate.rx;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.metamodel.model.creation.internal.PersisterClassResolverInitiator;
import org.hibernate.rx.service.RxRuntimeModelDescriptorResolver;

import org.hibernate.testing.junit5.SessionFactoryBasedFunctionalTest;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Timeout( 60_000 ) // 1 H, I need to debug
@ExtendWith(VertxExtension.class)
public class ReactiveSessionTest extends SessionFactoryBasedFunctionalTest {

	RxHibernateSession session = null;

	@BeforeEach
	public void setupSession() {
		// TODO: When ready, create this using ServiceRegistryBootstrap
		session = getSessionFactoryProducer()
				.produceSessionFactory()
				.unwrap( RxHibernateSessionFactory.class )
				.openRxSession();
	}

	@AfterEach
	public void closeSession() {
		session.close();
	}

	@Override
	protected boolean exportSchema() {
		return true;
	}

	@Override
	protected void applySettings(StandardServiceRegistryBuilder builer) {
		// TODO: Move this somewhere else in the implementation
		builer.applySetting( PersisterClassResolverInitiator.IMPL_NAME, RxRuntimeModelDescriptorResolver.class.getName() );
		builer.applySetting( AvailableSettings.DIALECT, "org.hibernate.dialect.PostgreSQL9Dialect" );
		builer.applySetting( AvailableSettings.DRIVER, "org.postgresql.Driver" );
		builer.applySetting( AvailableSettings.USER, "hibernate-rx" );
		builer.applySetting( AvailableSettings.PASS, "hibernate-rx" );
		builer.applySetting( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx" );
	}

	@Override
	protected void applyMetadataSources(MetadataSources metadataSources) {
		metadataSources.addAnnotatedClass( GuineaPig.class );
	}

	@Ignore
	@Test
	public void testRegularPersist() {
		sessionFactoryScope().inTransaction( (session) -> {
			session.persist( new GuineaPig( 2, "Aloi" ) );
		} );
	}

	@Ignore
	@Test
	public void testRegularFind() {
		GuineaPig aloi = new GuineaPig( 2, "Aloi" );
		sessionFactoryScope().inTransaction( (session) -> {
			session.persist( aloi );
		} );
		sessionFactoryScope().inTransaction( (session) -> {
			GuineaPig guineaPig = session.find( GuineaPig.class, 2 );
			assertThat( guineaPig ).isNotNull();
		} );
	}

	@Test
	public void testReactivePersist(VertxTestContext testContext) throws Exception {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		CompletionStage<Void> persistStage = session.reactive()
				.persist( mibbles );

		persistStage.whenComplete( (pig, err) -> {
			assertAsync( testContext, () ->
					assertAll(
							() -> assertThat( pig ).isNull(),
							() -> assertThat( err ).isNull()
					) );
		} );
	}

	private void assertNoError(VertxTestContext testContext, CompletionStage<?> stage) {
		stage.whenComplete( (res, err) -> {
			if (err != null) {
				testContext.failNow( err );
			}
		} );
	}

	@Test
	public void testReactivePersitstAndThenFind(VertxTestContext testContext) throws Exception {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		Function<RxSession, CompletionStage<Void>> persistConsumer = (rxSession) -> {
			return rxSession.persist( mibbles );
		};

		Function<ReactiveTransaction, CompletionStage<Void>> persist = tx -> {
			return tx.runAsync( persistConsumer )
					.thenApply( result -> {
						return true;
					} )
					.thenApply( (commitable) -> {
						tx.commit();
						return null;
					} );
		};

		CompletionStage<ReactiveTransaction> txStage = session.reactive().beginTransaction();
		CompletionStage<Void> persistStage = txStage.thenCompose( persist );

		persistStage
				.toCompletableFuture()
				.get();

		session.clear();
		session.reactive().find( GuineaPig.class, mibbles.getId() )
					.whenComplete( (pig, err) ->
						assertAsync( testContext, () -> assertAll(
								()-> assertThat(pig).hasValue( mibbles ),
								()-> assertThat(err).isNull() ) ) );
	}

	private void assertAsync(VertxTestContext ctx, Runnable r) {
		try {
			r.run();
			ctx.completeNow();
		}
		catch ( Throwable t) {
			ctx.failNow( t );
		}
	}

	@Entity
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(String name) {
			this.id = new Random().nextInt();
			this.name = name;
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
