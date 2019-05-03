package org.hibernate.rx.sql.ast.consume.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.model.relational.spi.PhysicalTable;
import org.hibernate.sql.ast.consume.SyntaxException;
import org.hibernate.sql.ast.consume.spi.SqlAstWalker;
import org.hibernate.sql.ast.tree.expression.ColumnReference;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.GenericParameter;
import org.hibernate.sql.ast.tree.insert.InsertStatement;
import org.hibernate.sql.ast.tree.update.Assignment;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBinding;

import io.reactiverse.pgclient.PgPreparedQuery;

public class SqlInsertToRxInsertConverter extends AbstractSqlAstToRxOperationConverter
		implements SqlAstWalker {

	private final List<RxParameterBinder> rxParameterBinders = new ArrayList<>( 0 );

	protected SqlInsertToRxInsertConverter(SessionFactoryImplementor sessionFactory) {
		super( sessionFactory );
	}

	public static RxOperation createRxInsert(InsertStatement sqlAst, SessionFactoryImplementor sessionFactory) {
		final SqlInsertToRxInsertConverter walker = new SqlInsertToRxInsertConverter( sessionFactory );
		walker.processStatement( sqlAst );
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

	@Override
	public void visitAssignment(Assignment assignment) {
		throw new SyntaxException( "Encountered unexpected assignment clause" );
	}

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

	private void processStatement(InsertStatement sqlAst) {
		appendSql( "insert into " );

		final PhysicalTable targetTable = (PhysicalTable) sqlAst.getTargetTable().getTable();
		final String tableName = getSessionFactory().getJdbcServices()
				.getJdbcEnvironment()
				.getQualifiedObjectNameFormatter()
				.format(
						targetTable.getQualifiedTableName(),
						getSessionFactory().getJdbcServices().getJdbcEnvironment().getDialect()
				);

		appendSql( tableName );

		// todo (6.0) : need to render the target column list
		// 		for now we do not render


		appendSql( " (" );

		boolean firstPass = true;
		for ( ColumnReference columnReference : sqlAst.getTargetColumnReferences() ) {
			if ( firstPass ) {
				firstPass = false;
			}
			else {
				appendSql( ", " );
			}

			visitColumnReference( columnReference );
		}

		appendSql( ") values (" );

		firstPass = true;
		for ( Expression expression : sqlAst.getValues() ) {
			if ( firstPass ) {
				firstPass = false;
			}
			else {
				appendSql( ", " );
			}

			expression.accept( this );
		}

		appendSql( ")" );
	}

}
