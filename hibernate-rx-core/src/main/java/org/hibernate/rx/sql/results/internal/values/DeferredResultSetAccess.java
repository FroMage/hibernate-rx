/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.results.internal.values;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.CoreLogging;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.sql.ast.consume.spi.RxParameterBinder;
import org.hibernate.rx.sql.ast.consume.spi.RxSelect;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.internal.values.AbstractResultSetAccess;

import org.jboss.logging.Logger;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgResult;
import io.reactiverse.pgclient.Tuple;
import io.reactiverse.pgclient.impl.ArrayTuple;

public class DeferredResultSetAccess extends AbstractResultSetAccess {
	private static final Logger log = CoreLogging.logger( DeferredResultSetAccess.class );

	private final RxSelect jdbcSelect;
	private final ExecutionContext executionContext;
	private final Function<String, PreparedStatement> statementCreator;

	private PreparedStatement preparedStatement;

	// TODO: I think this should be something like CompletionStage<ResultSet>
	private ResultSet resultSet;

	public DeferredResultSetAccess(
			RxSelect jdbcSelect,
			ExecutionContext executionContext,
			Function<String, PreparedStatement> statementCreator) {
		super( executionContext.getSession() );
		this.executionContext = executionContext;
		this.jdbcSelect = jdbcSelect;
		this.statementCreator = statementCreator;
	}

	// TODO: It needs to return a reactive type or the ResultSet will always be null
	@Override
	public ResultSet getResultSet() {
		if ( resultSet == null ) {
			executeQuery();
		}
		return resultSet;
	}

	@Override
	public SessionFactoryImplementor getFactory() {
		return executionContext.getSession().getFactory();
	}


	private RxConnectionPoolProvider reactivePoolProvider(ExecutionContext executionContext) {
		return executionContext.getSession()
				.getSessionFactory()
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );
	}

	private void executeQuery() {
		final String sql = jdbcSelect.getSql();
		log.tracef( "Executing query to retrieve ResultSet : %s", sql );


		execute( jdbcSelect, executionContext, new CompletableFuture<>() );
	}

	private Tuple parameters(RxSelect select) {
		Tuple tuple = new ArrayTuple( select.getParameterBinders().size() );
		for ( RxParameterBinder param : select.getParameterBinders() ) {
			tuple.addValue( param.getBindValue() );
		}
		return tuple;
	}

	// TODO: This method should return the query stage, not having it as a parameter
	public void execute(RxSelect select, ExecutionContext executionContext, CompletionStage<Object> queryStage) {
		final RxSession rxSession = (RxSession) executionContext.getSession();
		RxConnectionPoolProvider poolProvider = reactivePoolProvider( executionContext );
		RxConnection connection = poolProvider.getConnection();
		connection.unwrap( PgPool.class ).getConnection( ar1 -> {
			if ( ar1.succeeded() ) {
				PgConnection pgConnection = ar1.result();
				Tuple tuple = parameters( select );
				pgConnection.preparedQuery(
						select.getSql(),
						tuple,
						ar2 -> {
							pgConnection.close();
							if ( ar2.succeeded() ) {
								final PgResult rowSet = ar2.result();
								// Right now we only run Insert or Delete
								queryStage.toCompletableFuture().complete( rowSet );
							}
							else {
								queryStage.toCompletableFuture().completeExceptionally( ar2.cause() );
							}
						}
				);
			}
			else {
				queryStage.toCompletableFuture().completeExceptionally( ar1.cause() );
			}
		} );
	}

	@Override
	public void release() {

	}
}
