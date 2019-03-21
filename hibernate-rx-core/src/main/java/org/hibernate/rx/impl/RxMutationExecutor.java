package org.hibernate.rx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.sql.exec.spi.RxMutation;
import org.hibernate.rx.sql.exec.spi.RxParameterBinder;
import org.hibernate.sql.exec.spi.ExecutionContext;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPreparedQuery;
import io.reactiverse.pgclient.PgRowSet;

public class RxMutationExecutor {

	public void execute(RxMutation operation, ExecutionContext executionContext, CompletionStage<Void> stage) {
		RxConnectionPoolProvider poolProvider = executionContext.getSession()
				.getSessionFactory()
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		RxConnection connection = poolProvider.getConnection();
		connection.unwrap(PgPool.class).getConnection(ar1 -> {
				PgConnection pgConnection = ar1.result();
				pgConnection.prepare(operation.getSql(), (ar2) -> {
					if (ar2.succeeded()) {
						PgPreparedQuery preparedQuery = ar2.result();
						// TODO: Set parameters
						preparedQuery.execute((queryResult) -> {
							final PgRowSet result = queryResult.result();
							stage.toCompletableFuture().complete( null );
							pgConnection.close();
						});
					}
				});
			});
	}
}
