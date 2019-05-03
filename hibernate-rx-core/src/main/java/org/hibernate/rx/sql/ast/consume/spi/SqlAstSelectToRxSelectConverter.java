/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.ast.consume.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.sql.exec.internal.RxSelectImpl;
import org.hibernate.sql.ast.consume.SqlAstPrinter;
import org.hibernate.sql.ast.consume.spi.SqlSelectAstWalker;
import org.hibernate.sql.ast.produce.spi.SqlAstSelectDescriptor;
import org.hibernate.sql.ast.tree.SqlAstTreeLogger;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.StandardResultSetMappingDescriptor;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.type.descriptor.spi.SqlTypeDescriptorIndicators;

import io.reactiverse.pgclient.PgPreparedQuery;

/**
 * The final phase of query translation.  Here we take the SQL-AST an
 * "interpretation".  For a select query, that means an instance of
 * {@link RxSelect}.
 */
public class SqlAstSelectToRxSelectConverter
		extends AbstractSqlAstToRxOperationConverter
		implements SqlSelectAstWalker, SqlTypeDescriptorIndicators {

	private final List<RxParameterBinder> rxParameterBinders = new ArrayList<>( 0 );

	/**
	 * Perform interpretation of a select query, returning the SqlSelectInterpretation
	 *
	 * @return The interpretation result
	 */
	public static RxSelect interpret(QuerySpec querySpec, SessionFactoryImplementor sessionFactory) {
		final SqlAstSelectToRxSelectConverter walker = new SqlAstSelectToRxSelectConverter( sessionFactory );
		walker.visitQuerySpec( querySpec );
		return new RxSelectImpl(
				walker.getSql(),
				walker.getRxParameterBinders(),
				new StandardResultSetMappingDescriptor(
						querySpec.getSelectClause().getSqlSelections(),
						Collections.emptyList()
				),
				walker.getAffectedTableNames()
		);
	}

	public static RxSelect interpret(SqlAstSelectDescriptor sqlSelectPlan, SessionFactoryImplementor sessionFactory) {
		final SelectStatement sqlAstStatement = sqlSelectPlan.getSqlAstStatement();
		if ( SqlAstTreeLogger.DEBUG_ENABLED ) {
			SqlAstPrinter.print( sqlAstStatement );
		}

		final SqlAstSelectToRxSelectConverter walker = new SqlAstSelectToRxSelectConverter( sessionFactory );

		walker.visitSelectQuery( sqlSelectPlan.getSqlAstStatement() );
		return new RxSelectImpl(
				walker.getSql(),
				walker.getRxParameterBinders(),
				new StandardResultSetMappingDescriptor(
						sqlSelectPlan.getSqlAstStatement().getQuerySpec().getSelectClause().getSqlSelections(),
						sqlSelectPlan.getQueryResults()
				),
				sqlSelectPlan.getAffectedTableNames()
		);
	}

	private SqlAstSelectToRxSelectConverter(SessionFactoryImplementor sessionFactory) {
		super( sessionFactory );
	}

	public List<RxParameterBinder> getRxParameterBinders() {
		return rxParameterBinders;
	}

	@Override
	public void visitSelectQuery(SelectStatement selectQuery) {
		visitQuerySpec( selectQuery.getQuerySpec() );
	}

	@Override
	protected void visitJdbcParameterBinder(JdbcParameterBinder jdbcParameterBinder) {
		super.visitJdbcParameterBinder( jdbcParameterBinder  );

		RxParameterBinder rxBinder = new RxParameterBinder() {
			@Override
			public int bindParameterValue(
					PgPreparedQuery statement, int startPosition, ExecutionContext executionContext) {
				return 1;
			}

			@Override
			public Object getBindValue() {
				return null;
			}
		};

		rxParameterBinders.add( rxBinder );
	}

}
