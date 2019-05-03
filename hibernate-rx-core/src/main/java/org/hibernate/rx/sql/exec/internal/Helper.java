/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.exec.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.produce.internal.StandardSqlExpressionResolver;
import org.hibernate.sql.ast.produce.spi.ColumnReferenceQualifier;
import org.hibernate.sql.ast.produce.spi.SqlAstCreationContext;
import org.hibernate.sql.ast.produce.spi.SqlExpressionResolver;
import org.hibernate.sql.ast.produce.sqm.spi.Callback;
import org.hibernate.sql.exec.spi.RowTransformer;
import org.hibernate.sql.results.SqlResultsLogger;
import org.hibernate.sql.results.internal.RowReaderStandardImpl;
import org.hibernate.sql.results.internal.values.JdbcValues;
import org.hibernate.sql.results.spi.AssemblerCreationState;
import org.hibernate.sql.results.spi.DomainResultAssembler;
import org.hibernate.sql.results.spi.Initializer;
import org.hibernate.sql.results.spi.RowReader;

public class Helper {

	public static final SqlExpressionResolver SQL_EXPRESSION_RESOLVER = new StandardSqlExpressionResolver(
			() -> null,
			expression -> expression,
			(expression, sqlSelection) -> {
			}
	);

	public static <R> RowReader<R> createRowReader(
			SessionFactoryImplementor sessionFactory,
			Callback callback,
			RowTransformer<R> rowTransformer,
			JdbcValues jdbcValues) {
		final List<Initializer> initializers = new ArrayList<>();

		final List<DomainResultAssembler> assemblers = jdbcValues.getResultSetMapping().resolveAssemblers(
				getInitializerConsumer( initializers ),
				new AssemblerCreationState() {
					@Override
					public LoadQueryInfluencers getLoadQueryInfluencers() {
						return LoadQueryInfluencers.NONE;
					}

					@Override
					public SqlExpressionResolver getSqlExpressionResolver() {
						return SQL_EXPRESSION_RESOLVER;
					}

					@Override
					public SqlAstCreationContext getSqlAstCreationContext() {
						return sessionFactory;
					}

					@Override
					public LockOptions getLockOptions() {
						return null;
					}

					@Override
					public boolean shouldCreateShallowEntityResult() {
						return false;
					}

					@Override
					public ColumnReferenceQualifier getCurrentColumnReferenceQualifier() {
						throw new UnsupportedOperationException(  );
					}
				}
		);

		return new RowReaderStandardImpl<>(
				assemblers,
				initializers,
				rowTransformer,
				callback
		);
	}

	private static Consumer<Initializer> getInitializerConsumer(List<Initializer> initializers) {
		if ( SqlResultsLogger.INSTANCE.isDebugEnabled() ) {
			return initializer -> {
				// noinspection Convert2MethodRef
				SqlResultsLogger.INSTANCE.debug( "Adding initializer : " + initializer );
				initializers.add( initializer );
			};
		}
		else {
			return initializer -> {
				// noinspection Convert2MethodRef
				initializers.add( initializer );
			};
		}
	}
}
