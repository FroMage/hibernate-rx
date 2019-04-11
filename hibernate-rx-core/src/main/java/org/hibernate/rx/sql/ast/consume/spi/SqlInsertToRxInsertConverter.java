package org.hibernate.rx.sql.ast.consume.spi;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.model.relational.spi.PhysicalTable;
import org.hibernate.metamodel.model.relational.spi.Table;
import org.hibernate.sql.ast.consume.SyntaxException;
import org.hibernate.sql.ast.consume.spi.AbstractSqlAstWalker;
import org.hibernate.sql.ast.consume.spi.SqlAstWalker;
import org.hibernate.sql.ast.tree.spi.InsertStatement;
import org.hibernate.sql.ast.tree.spi.assign.Assignment;
import org.hibernate.sql.ast.tree.spi.expression.ColumnReference;
import org.hibernate.sql.ast.tree.spi.expression.Expression;
import org.hibernate.sql.ast.tree.spi.expression.GenericParameter;
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
		/*return new JdbcInsert() {
			public boolean isKeyGenerationEnabled() {
				return false;
			}

			public String getSql() {
				return walker.getSql();
			}

			public List<JdbcParameterBinder> getParameterBinders() {
				return walker.getParameterBinders();
			}

			public Set<String> getAffectedTableNames() {
				return walker.getAffectedTableNames();
			}
		};*/
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
		this.appendSql( "insert into " );
		PhysicalTable targetTable = (PhysicalTable) sqlAst.getTargetTable().getTable();
		String tableName = this.getSessionFactory()
				.getJdbcServices()
				.getJdbcEnvironment()
				.getQualifiedObjectNameFormatter()
				.format(
						targetTable.getQualifiedTableName(),
						this.getSessionFactory().getJdbcServices().getJdbcEnvironment().getDialect()
				);
		this.appendSql( tableName );
		this.appendSql( " (" );
		boolean firstPass = true;

		Iterator var5;
		ColumnReference columnReference;
		for ( var5 = sqlAst.getTargetColumnReferences().iterator(); var5.hasNext(); this.visitColumnReference(
				columnReference ) ) {
			columnReference = (ColumnReference) var5.next();
			if ( firstPass ) {
				firstPass = false;
			}
			else {
				this.appendSql( ", " );
			}
		}

		this.appendSql( ") values (" );
		firstPass = true;

		Expression expression;
		for ( var5 = sqlAst.getValues().iterator(); var5.hasNext(); expression.accept( this ) ) {
			expression = (Expression) var5.next();
			if ( firstPass ) {
				firstPass = false;
			}
			else {
				this.appendSql( ", " );
			}
		}

		this.appendSql( ")" );
	}
}
