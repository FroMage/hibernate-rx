package org.hibernate.rx.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.sql.exec.spi.RxMutation;
import org.hibernate.rx.sql.exec.spi.RxParameterBinder;
import org.hibernate.sql.exec.spi.ExecutionContext;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.impl.ArrayTuple;

public class RxMutationExecutor {

	public void execute(RxMutation operation, ExecutionContext executionContext, CompletionStage<?> operationStage) {
//		Just the insert for now
		final RxSession rxSession = (RxSession) executionContext.getSession();
		RxConnectionPoolProvider poolProvider = reactivePoolProvider( executionContext );
		RxConnection connection = poolProvider.getConnection();
		connection.unwrap( PgPool.class ).getConnection( ar1 -> {
			if ( ar1.succeeded() ) {
				PgConnection pgConnection = ar1.result();
				ArrayTuple tuple = parameters( operation );
				pgConnection.preparedQuery(
						operation.getSql(),
						tuple,
						ar2 -> {
							pgConnection.close();
							if ( ar2.succeeded() ) {
								final Object rowSet = ar2.result();
								// Right now we only run Insert or Delete
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

	private ArrayTuple parameters(RxMutation operation) {
		ArrayTuple tuple = new ArrayTuple( operation.getParameterBinders().size() );
		for ( RxParameterBinder param : operation.getParameterBinders() ) {
			tuple.addValue( param.getBindValue() );
		}
		return tuple;
	}

	private RxConnectionPoolProvider reactivePoolProvider(ExecutionContext executionContext) {
		return executionContext.getSession()
				.getSessionFactory()
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );
	}
}
