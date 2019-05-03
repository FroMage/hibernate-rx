/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.ast.consume.spi;

import java.util.HashSet;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.model.relational.spi.Table;
import org.hibernate.sql.ast.consume.SyntaxException;
import org.hibernate.sql.ast.consume.spi.AbstractSqlAstWalker;
import org.hibernate.sql.ast.tree.update.Assignment;

public class AbstractSqlAstToRxOperationConverter
		extends AbstractSqlAstWalker
		implements SqlAstToRxOperationConverter {

	private final Set<String> affectedTableNames = new HashSet<>();

	protected AbstractSqlAstToRxOperationConverter(SessionFactoryImplementor sessionFactory) {
		super( sessionFactory );
	}

	@Override
	public void visitAssignment(Assignment assignment) {
		throw new SyntaxException( "Encountered unexpected assignment clause" );
	}

	@Override
	public Set<String> getAffectedTableNames() {
		return affectedTableNames;
	}

	protected void registerAffectedTable(Table table) {
		affectedTableNames.add( table.getTableExpression() );
	}

	protected void registerAffectedTable(String tableExpression) {
		affectedTableNames.add( tableExpression );
	}
}
