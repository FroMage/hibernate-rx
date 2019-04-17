/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.exec.spi;

import java.util.List;
import java.util.stream.Stream;

import org.hibernate.Incubating;
import org.hibernate.ScrollMode;
import org.hibernate.query.spi.ScrollableResultsImplementor;
import org.hibernate.rx.sql.ast.consume.spi.RxSelect;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.RowTransformer;

/**
 * An executor for RxSelect operations.
 */
@Incubating
public interface RxSelectExecutor {

	// todo (6.0) : need to pass some form of JdbcValuesSourceProcessingOptions to list to be able to have it handle single entity loads -
	//		or just drop the form of loading an entity by passing an instance of itself as the one to load

	// todo (6.0) : Ideally we'd have a singular place (JdbcServices? ServiceRegistry?) to obtain these executors

	<R> List<R> list(
			RxSelect jdbcSelect,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer);

	<R> ScrollableResultsImplementor<R> scroll(
			RxSelect jdbcSelect,
			ScrollMode scrollMode,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer);

	<R> Stream<R> stream(
			RxSelect jdbcSelect,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer);
}
