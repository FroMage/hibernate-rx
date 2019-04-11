/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.ast.consume.spi;

import java.util.Collections;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.consume.spi.AbstractSqlAstToJdbcOperationConverter;
import org.hibernate.sql.ast.consume.spi.SqlSelectAstWalker;
import org.hibernate.sql.ast.produce.spi.SqlAstSelectDescriptor;
import org.hibernate.sql.ast.tree.spi.QuerySpec;
import org.hibernate.sql.ast.tree.spi.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcSelectImpl;
import org.hibernate.sql.exec.internal.StandardResultSetMappingDescriptor;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.type.descriptor.spi.SqlTypeDescriptorIndicators;

/**
 * The final phase of query translation.  Here we take the SQL-AST an
 * "interpretation".  For a select query, that means an instance of
 * {@link JdbcSelect}.
 *
 * @author Steve Ebersole
 */
public class SqlAstSelectToRxSelectConverter
		extends AbstractSqlAstToJdbcOperationConverter
		implements SqlSelectAstWalker, SqlTypeDescriptorIndicators {
	/**
	 * Perform interpretation of a select query, returning the SqlSelectInterpretation
	 *
	 * @return The interpretation result
	 */
	public static JdbcSelect interpret(QuerySpec querySpec, SessionFactoryImplementor sessionFactory) {
		final SqlAstSelectToRxSelectConverter walker = new SqlAstSelectToRxSelectConverter( sessionFactory );
		walker.visitQuerySpec( querySpec );
		return new JdbcSelectImpl(
				walker.getSql(),
				walker.getParameterBinders(),
				new StandardResultSetMappingDescriptor(
						querySpec.getSelectClause().getSqlSelections(),
						Collections.emptyList()
				),
				walker.getAffectedTableNames()
		);
	}

	public static JdbcSelect interpret(SqlAstSelectDescriptor sqlSelectPlan, SessionFactoryImplementor sessionFactory) {
		final SqlAstSelectToRxSelectConverter walker = new SqlAstSelectToRxSelectConverter( sessionFactory );

		walker.visitSelectQuery( sqlSelectPlan.getSqlAstStatement() );
		return new JdbcSelectImpl(
				walker.getSql(),
				walker.getParameterBinders(),
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

	@Override
	public void visitSelectQuery(SelectStatement selectQuery) {
		visitQuerySpec( selectQuery.getQuerySpec() );
	}
}
