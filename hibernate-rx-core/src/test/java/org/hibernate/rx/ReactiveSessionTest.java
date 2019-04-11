package org.hibernate.rx;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.metamodel.model.creation.internal.PersisterClassResolverInitiator;
import org.hibernate.rx.service.RxRuntimeModelDescriptorResolver;

import org.hibernate.testing.junit5.SessionFactoryBasedFunctionalTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Timeout(3600_000) // 1 H, I need to debug
@ExtendWith(VertxExtension.class)
public class ReactiveSessionTest extends SessionFactoryBasedFunctionalTest {

	RxSession session = null;

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
		builer.applySetting(
				PersisterClassResolverInitiator.IMPL_NAME,
				RxRuntimeModelDescriptorResolver.class.getName()
		);
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

	@Test
	public void testReactivePersist(VertxTestContext testContext) throws Exception {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		CompletionStage<Void> persistStage = session.persistAsync( mibbles );

		persistStage.whenComplete( (nothing, err) -> {
			assertAsync( testContext, () ->
					assertAll(
							() -> assertThat( nothing ).isNull(),
							() -> assertThat( err ).isNull()
					) );
		} );
	}

	@Test
	public void testFindWithNull(VertxTestContext testContext) throws Exception {
		session.findAsync( GuineaPig.class, 22 ).whenComplete( (result, err) -> {
			assertAsync( testContext, () ->
					assertAll(
							() -> assertThat( result ).isNotPresent(),
							() -> assertThat( err ).isNull()
					) );
		} );
	}

	@Test
	public void testFind(VertxTestContext testContext) throws Exception {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		CompletionStage<Void> persistStage = session.persistAsync( mibbles );
		persistStage
				.exceptionally( err -> {
					testContext.failNow( err );
					return null;
				} )
				.thenAccept( ignore -> {
					session.findAsync( GuineaPig.class, 22 ).whenComplete( (result, err) -> {
						assertAsync( testContext, () ->
								assertAll(
										() -> assertThat( result ).hasValue( mibbles ),
										() -> assertThat( err ).isNull()
								) );
			} );
		} );
	}

	@Test
	public void testRemove(VertxTestContext testContext) throws Exception {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		CompletionStage<Void> persistStage = session.persistAsync( mibbles );

		persistStage.whenComplete( (nothing, err1) -> {
			session.removeAsync( mibbles ).whenComplete( (ignore, err) -> {
				assertAsync( testContext, () ->
						assertAll(
								() -> assertThat( ignore ).isNull(),
								() -> assertThat( err ).isNull()
						) );
			} ).thenAccept( (ignore)-> {
			} );

		} );
	}

	@Test
	public void testReactivePersitstAndThenFind(VertxTestContext testContext) throws Exception {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		CompletionStage<Void> persistStage = session.persistAsync( mibbles );

		persistStage.thenAccept( ignore -> {
			session.findAsync( GuineaPig.class, mibbles.getId() )
					.whenComplete( (pig, err) ->
										   assertAsync( testContext, () -> assertAll(
												   () -> assertThat( pig ).hasValue( mibbles ),
												   () -> assertThat( err ).isNull()
										   ) ) );
		} );
	}

	private void assertAsync(VertxTestContext ctx, Runnable r) {
		try {
			r.run();
			ctx.completeNow();
		}
		catch (Throwable t) {
			ctx.failNow( t );
		}
	}

	@Entity(name = "GuineaPig")
	@Table(name = "GuineaPig")
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
