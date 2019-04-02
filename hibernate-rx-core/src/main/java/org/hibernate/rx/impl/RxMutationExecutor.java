package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.persistence.EntityTransaction;

import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.sql.exec.spi.RxMutation;
import org.hibernate.sql.exec.spi.ExecutionContext;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPreparedQuery;
import io.reactiverse.pgclient.PgRowSet;

public class RxMutationExecutor {

	public CompletionStage<?> execute(RxMutation operation, ExecutionContext executionContext, CompletionStage<?> stage) {
		final CompletionStage<Object> resultStage = new CompletableFuture<>();
		RxHibernateSession rxHibernateSession = (RxHibernateSession) executionContext.getSession();
		RxSession rxSession = rxHibernateSession.reactive();
		Consumer<RxSession> consumer = (RxSession) -> {
			RxConnectionPoolProvider poolProvider = reactivePoolProvider( executionContext );

			RxConnection connection = poolProvider.getConnection();
			connection.unwrap( PgPool.class ).getConnection( ar1 -> {
				PgConnection pgConnection = ar1.result();
				pgConnection.prepare( operation.getSql(), (ar2) -> {
					if ( ar2.succeeded() ) {
						PgPreparedQuery preparedQuery = ar2.result();
						// TODO: Set parameters
						preparedQuery.execute( (queryResult) -> {
							try {
								final PgRowSet rowSet = queryResult.result();
								resultStage.toCompletableFuture().complete( rowSet );
							}
							finally {
								pgConnection.close();
							}
						} );
					}
				} );
				pgConnection.
			} );
		};
		rxHibernateSession.reactive().inTransaction( consumer );
		return null;
	}

	private RxConnectionPoolProvider reactivePoolProvider(ExecutionContext executionContext) {
		return executionContext.getSession()
				.getSessionFactory()
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );
	}
}
