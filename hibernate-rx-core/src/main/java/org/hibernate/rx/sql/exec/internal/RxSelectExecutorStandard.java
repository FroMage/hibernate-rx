package org.hibernate.rx.sql.exec.internal;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.hibernate.CacheMode;
import org.hibernate.ScrollMode;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.query.internal.ScrollableResultsIterator;
import org.hibernate.query.spi.ScrollableResultsImplementor;
import org.hibernate.rx.sql.ast.consume.spi.RxSelect;
import org.hibernate.rx.sql.exec.spi.RxSelectExecutor;
import org.hibernate.rx.sql.results.internal.values.DeferredResultSetAccess;
import org.hibernate.sql.exec.internal.ListResultsConsumer;
import org.hibernate.sql.exec.internal.ResultsConsumer;
import org.hibernate.sql.exec.internal.ScrollableResultsConsumer;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.RowTransformer;
import org.hibernate.sql.results.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.internal.RowProcessingStateStandardImpl;
import org.hibernate.sql.results.internal.values.JdbcValues;
import org.hibernate.sql.results.internal.values.JdbcValuesCacheHit;
import org.hibernate.sql.results.internal.values.JdbcValuesResultSetImpl;
import org.hibernate.sql.results.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.ResultSetAccess;
import org.hibernate.sql.results.spi.ResultSetMapping;
import org.hibernate.sql.results.spi.RowReader;

import org.jboss.logging.Logger;

/**
 * See {@link org.hibernate.sql.exec.internal.JdbcSelectExecutorStandardImpl}
 */
public class RxSelectExecutorStandard implements RxSelectExecutor {
	// todo (6.0) : Make resolving these executors swappable - JdbcServices?
	//		Since JdbcServices is just a "composition service", this is actually
	//		a very good option...

	// todo (6.0) : where do affected-table-names get checked for up-to-date?
	//		who is responsible for that?  Here?

	/**
	 * Singleton access
	 */
	public static final RxSelectExecutorStandard INSTANCE = new RxSelectExecutorStandard();

	private static final Logger log = Logger.getLogger( RxSelectExecutorStandard.class );

	@Override
	public <R> List<R> list(
			RxSelect rxSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer) {
		return executeQuery(
				rxSelect,
				executionContext,
				rowTransformer,
				(sql) -> executionContext.getSession()
						.getJdbcCoordinator()
						.getStatementPreparer()
						.prepareStatement( sql ),
				ListResultsConsumer.instance()
		);
	}

	@Override
	public <R> ScrollableResultsImplementor<R> scroll(
			RxSelect rxSelect,
			ScrollMode scrollMode,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer) {
		return executeQuery(
				rxSelect,
				executionContext,
				rowTransformer,
				(sql) -> executionContext.getSession().getJdbcCoordinator().getStatementPreparer().prepareQueryStatement(
						sql,
						scrollMode,
						true
				),
				ScrollableResultsConsumer.instance()
		);
	}

	@Override
	public <R> Stream<R> stream(
			RxSelect rxSelect,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer) {
		final ScrollableResultsImplementor<R> scrollableResults = scroll(
				rxSelect,
				ScrollMode.FORWARD_ONLY,
				executionContext,
				rowTransformer
		);
		final ScrollableResultsIterator<R> iterator = new ScrollableResultsIterator<>( scrollableResults );
		final Spliterator<R> spliterator = Spliterators.spliteratorUnknownSize( iterator, Spliterator.NONNULL );

		final Stream<R> stream = StreamSupport.stream( spliterator, false );
		return stream.onClose( scrollableResults::close );
	}

	private enum ExecuteAction {
		EXECUTE_QUERY,
	}

	private <T, R> T executeQuery(
			RxSelect rxSelect,
			ExecutionContext executionContext,
			RowTransformer<R> rowTransformer,
			Function<String, PreparedStatement> statementCreator,
			ResultsConsumer<T,R> resultsConsumer) {

		final JdbcValues jdbcValues = resolveJdbcValuesSource(
				rxSelect,
				executionContext,
				new DeferredResultSetAccess(
						rxSelect,
						executionContext,
						statementCreator
				)
		);

		/*
		 * Processing options effectively are only used for entity loading.  Here we don't need these values.
		 */
		final JdbcValuesSourceProcessingOptions processingOptions = new JdbcValuesSourceProcessingOptions() {
			@Override
			public Object getEffectiveOptionalObject() {
				return null;
			}

			@Override
			public String getEffectiveOptionalEntityName() {
				return null;
			}

			@Override
			public Serializable getEffectiveOptionalId() {
				return null;
			}

			@Override
			public boolean shouldReturnProxies() {
				return true;
			}
		};

		final JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState =
				new JdbcValuesSourceProcessingStateStandardImpl( executionContext, processingOptions );

		final List<AfterLoadAction> afterLoadActions = new ArrayList<>();

		final RowReader<R> rowReader = Helper.createRowReader(
				executionContext.getSession().getSessionFactory(),
				afterLoadActions::add,
				rowTransformer,
				jdbcValues
		);

		final RowProcessingStateStandardImpl rowProcessingState = new RowProcessingStateStandardImpl(
				jdbcValuesSourceProcessingState,
				executionContext.getQueryOptions(),
				rowReader,
				jdbcValues
		);

		final T result = resultsConsumer.consume(
				jdbcValues,
				executionContext.getSession(),
				processingOptions,
				jdbcValuesSourceProcessingState,
				rowProcessingState,
				rowReader
		);

		for ( AfterLoadAction afterLoadAction : afterLoadActions ) {
			afterLoadAction.afterLoad( executionContext.getSession() );
		}


		return result;
	}

	@SuppressWarnings("unchecked")
	private JdbcValues resolveJdbcValuesSource(
			RxSelect rxSelect,
			ExecutionContext executionContext,
			ResultSetAccess resultSetAccess) {
		final List<Object[]> cachedResults;

		final boolean queryCacheEnabled = executionContext.getSession().getFactory().getSessionFactoryOptions().isQueryCacheEnabled();
		final CacheMode cacheMode = resolveCacheMode(  executionContext );

		final ResultSetMapping resultSetMapping = rxSelect.getResultSetMappingDescriptor()
				.resolve( resultSetAccess, executionContext.getSession().getSessionFactory() );

		final QueryKey queryResultsCacheKey;

		if ( queryCacheEnabled && executionContext.getQueryOptions().getCacheMode().isGetEnabled() ) {
			log.debugf( "Reading Query result cache data per CacheMode#isGetEnabled [%s]", cacheMode.name() );

			final QueryResultsCache queryCache = executionContext.getSession().getFactory()
					.getCache()
					.getQueryResultsCache( executionContext.getQueryOptions().getResultCacheRegionName() );

			// todo (6.0) : not sure that it is at all important that we account for QueryResults
			//		these cached values are "lower level" than that, representing the
			// 		"raw" JDBC values.
			//
			// todo (6.0) : relatedly ^^, pretty sure that SqlSelections are also irrelevant

			queryResultsCacheKey = QueryKey.from(
					rxSelect.getSql(),
					executionContext.getQueryOptions().getLimit(),
					executionContext.getDomainParameterBindingContext().getQueryParameterBindings(),
					executionContext.getSession()
			);

			cachedResults = queryCache.get(
					// todo (6.0) : QueryCache#get takes the `queryResultsCacheKey` see tat discussion above
					queryResultsCacheKey,
					// todo (6.0) : `querySpaces` and `session` make perfect sense as args, but its odd passing those into this method just to pass along
					//		atm we do not even collect querySpaces, but we need to
					rxSelect.getAffectedTableNames(),
					executionContext.getSession()
			);

			// todo (6.0) : `querySpaces` and `session` are used in QueryCache#get to verify "up-to-dateness" via UpdateTimestampsCache
			//		better imo to move UpdateTimestampsCache handling here and have QueryCache be a simple access to
			//		the underlying query result cache region.
			//
			// todo (6.0) : if we go this route (^^), still beneficial to have an abstraction over different UpdateTimestampsCache-based
			//		invalidation strategies - QueryCacheInvalidationStrategy
		}
		else {
			log.debugf( "Skipping reading Query result cache data: cache-enabled = %s, cache-mode = %s",
						queryCacheEnabled,
						cacheMode.name()
			);
			cachedResults = null;
			queryResultsCacheKey = null;
		}

		if ( cachedResults == null || cachedResults.isEmpty() ) {
			return new JdbcValuesResultSetImpl(
					resultSetAccess,
					queryResultsCacheKey,
					executionContext.getQueryOptions(),
					resultSetMapping,
					executionContext
			);
		}
		else {
			return new JdbcValuesCacheHit(
					cachedResults,
					resultSetMapping
			);
		}
	}

	private CacheMode resolveCacheMode(ExecutionContext executionContext) {
		CacheMode cacheMode = executionContext.getQueryOptions().getCacheMode();
		if ( cacheMode != null ) {
			return cacheMode;
		}

		cacheMode = executionContext.getSession().getCacheMode();
		if ( cacheMode != null ) {
			return cacheMode;
		}

		return CacheMode.NORMAL;
	}
}
