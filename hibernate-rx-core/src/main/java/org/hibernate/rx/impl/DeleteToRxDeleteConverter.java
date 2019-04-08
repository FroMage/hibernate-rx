package org.hibernate.rx.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.model.relational.spi.PhysicalTable;
import org.hibernate.rx.sql.exec.spi.RxMutation;
import org.hibernate.rx.sql.exec.spi.RxParameterBinder;
import org.hibernate.sql.ast.consume.spi.AbstractSqlAstToJdbcOperationConverter;
import org.hibernate.sql.ast.consume.spi.SqlMutationToJdbcMutationConverter;
import org.hibernate.sql.ast.tree.spi.DeleteStatement;
import org.hibernate.sql.ast.tree.spi.expression.GenericParameter;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBinding;

import io.reactiverse.pgclient.PgPreparedQuery;

// TODO: Create a Rx interface instead of a JDBC one
public class DeleteToRxDeleteConverter extends AbstractSqlAstToJdbcOperationConverter
		implements SqlMutationToJdbcMutationConverter {
	private final Set<String> affectedTableNames;

	protected DeleteToRxDeleteConverter(SessionFactoryImplementor sessionFactory, DeleteStatement statement) {
		super( sessionFactory );
		this.affectedTableNames = Collections.singleton(
				statement.getTargetTable().getTable().getTableExpression()
		);
	}

	private final List<RxParameterBinder> rxParameterBinders = new ArrayList<>( 0 );

	@Override
	protected void visitJdbcParameterBinder(JdbcParameterBinder jdbcParameterBinder) {
		getParameterBinders().add( jdbcParameterBinder );

		// todo (6.0) : ? wrap in cast function call if the literal occurs in SELECT (?based on Dialect?)

		appendSql( "$" + getParameterBinders().size() );
	}

	@Override
	public void visitGenericParameter(GenericParameter parameter) {
		super.visitGenericParameter( parameter );
		final Object bindValue = ( (JdbcParameterBinding) parameter ).getBindValue();

		RxParameterBinder rxBinder = new RxParameterBinder() {
			@Override
			public int bindParameterValue(
					PgPreparedQuery statement, int startPosition, ExecutionContext executionContext) {
				return 1;
			}

			@Override
			public Object getBindValue() {
				return bindValue;
			}
		};

		rxParameterBinders.add( rxBinder );
	}

	public List<RxParameterBinder> getRxParameterBinders() {
		return rxParameterBinders;
	}

	public static RxMutation createRxDelete(DeleteStatement statement, SessionFactoryImplementor sessionFactory) {
		final DeleteToRxDeleteConverter walker = new DeleteToRxDeleteConverter( sessionFactory, statement );
		walker.processDeleteStatement( statement );
		// TODO: Create specific class for insert? Ex: InsertRxMutation
		return new RxMutation() {
			@Override
			public String getSql() {
				return walker.getSql();
			}

			@Override
			public List<RxParameterBinder> getParameterBinders() {
				return walker.getRxParameterBinders();
			}

			@Override
			public Set<String> getAffectedTableNames() {
				return walker.getAffectedTableNames();
			}
		};
	}

	@Override
	public Set<String> getAffectedTableNames() {
		return affectedTableNames;
	}

	/**
	 * See org.hibernate.sql.ast.consume.spi.SqlDeleteToJdbcDeleteConverter
 	 */
	private void processDeleteStatement(DeleteStatement deleteStatement) {
		appendSql( "delete from " );

		final PhysicalTable targetTable = (PhysicalTable) deleteStatement.getTargetTable().getTable();
		final String tableName = getSessionFactory().getJdbcServices()
				.getJdbcEnvironment()
				.getQualifiedObjectNameFormatter()
				.format(
						targetTable.getQualifiedTableName(),
						getSessionFactory().getJdbcServices().getJdbcEnvironment().getDialect()
				);

		appendSql( tableName );

		if ( deleteStatement.getRestriction() != null ) {
			appendSql( " where " );
			deleteStatement.getRestriction().accept( this );
		}
	}
}
