package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

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
import io.reactiverse.pgclient.PgTransaction;

public class RxMutationExecutor {

	public void execute(RxMutation operation, ExecutionContext executionContext, CompletionStage<?> operationStage) {
//		Just the insert for now
		final RxHibernateSession rxHibernateSession = (RxHibernateSession) executionContext.getSession();
		RxSession rxSession = rxHibernateSession.reactive();
		RxConnectionPoolProvider poolProvider = reactivePoolProvider( executionContext );
		RxConnection connection = poolProvider.getConnection();
		connection.unwrap( PgPool.class ).getConnection( ar1 -> {
			if ( ar1.succeeded() ) {
				PgConnection pgConnection = ar1.result();
				pgConnection.preparedQuery(
						"insert into ReactiveSessionTest$GuineaPig (id, name) values ( 22, 'Mibbles')",
						ar2 -> {
							pgConnection.close();
							if ( ar2.succeeded() ) {
								final Object rowSet = ar2.result();
								operationStage.toCompletableFuture().complete( null );
							}
							else {
								operationStage.toCompletableFuture().completeExceptionally( ar2.cause() );
							}
						}
				);
			}
			else {
				operationStage.toCompletableFuture().completeExceptionally( ar1.cause() );
			}
		} );

	}

	private RxConnectionPoolProvider reactivePoolProvider(ExecutionContext executionContext) {
		return executionContext.getSession()
				.getSessionFactory()
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );
	}
}
