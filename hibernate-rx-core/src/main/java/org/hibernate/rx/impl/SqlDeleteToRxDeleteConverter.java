package org.hibernate.rx.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.model.relational.spi.PhysicalTable;
import org.hibernate.rx.sql.ast.consume.spi.AbstractSqlAstToRxOperationConverter;
import org.hibernate.rx.sql.ast.consume.spi.RxOperation;
import org.hibernate.rx.sql.ast.consume.spi.RxParameterBinder;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.consume.spi.SqlAstWalker;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.expression.GenericParameter;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBinding;

import io.reactiverse.pgclient.PgPreparedQuery;

public class SqlDeleteToRxDeleteConverter extends AbstractSqlAstToRxOperationConverter
		implements SqlAstWalker {

	protected SqlDeleteToRxDeleteConverter(SessionFactoryImplementor sessionFactory, DeleteStatement statement) {
		super( sessionFactory );
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

	public static RxOperation createRxDelete(DeleteStatement statement, SessionFactoryImplementor sessionFactory) {
		final SqlDeleteToRxDeleteConverter walker = new SqlDeleteToRxDeleteConverter( sessionFactory, statement );
		walker.processDeleteStatement( statement );
		// TODO: Create specific class for insert? Ex: InsertRxMutation
		return new RxOperation() {
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
			getClauseStack().push( Clause.WHERE );
			try {
				appendSql( " where " );
				deleteStatement.getRestriction().accept( this );
			}
			finally {
				getClauseStack().pop();
			}
		}
	}
}
