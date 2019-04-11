/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.ast.consume.spi;

import org.hibernate.sql.exec.spi.ExecutionContext;

import io.reactiverse.pgclient.PgPreparedQuery;

public interface RxParameterBinder {

	// TODO: Replace PgPrepareQuery with a more general interface (Similar to PreparedStatements)
	int bindParameterValue(
			PgPreparedQuery statement,
			int startPosition,
			ExecutionContext executionContext); // throws SQLException;

	Object getBindValue();
}
