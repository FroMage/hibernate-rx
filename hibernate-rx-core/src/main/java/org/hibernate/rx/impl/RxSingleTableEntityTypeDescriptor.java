package org.hibernate.rx.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.HibernateException;
import org.hibernate.boot.model.domain.EntityMapping;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.internal.TemplateParameterBindingContext;
import org.hibernate.metamodel.model.creation.spi.RuntimeModelCreationContext;
import org.hibernate.metamodel.model.domain.internal.entity.SingleTableEntityTypeDescriptor;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.metamodel.model.domain.spi.IdentifiableTypeDescriptor;
import org.hibernate.metamodel.model.relational.spi.Column;
import org.hibernate.query.internal.QueryOptionsImpl;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.rx.event.CompletionStageExtraState;
import org.hibernate.rx.sql.ast.consume.spi.RxOperation;
import org.hibernate.rx.sql.ast.consume.spi.SqlInsertToRxInsertConverter;
import org.hibernate.sql.SqlExpressableType;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.consume.spi.SqlDeleteToJdbcDeleteConverter;
import org.hibernate.sql.ast.produce.spi.SqlAstDeleteDescriptor;
import org.hibernate.sql.ast.produce.sqm.spi.Callback;
import org.hibernate.sql.ast.tree.spi.DeleteStatement;
import org.hibernate.sql.ast.tree.spi.InsertStatement;
import org.hibernate.sql.ast.tree.spi.expression.ColumnReference;
import org.hibernate.sql.ast.tree.spi.expression.LiteralParameter;
import org.hibernate.sql.ast.tree.spi.from.TableReference;
import org.hibernate.sql.ast.tree.spi.predicate.Junction;
import org.hibernate.sql.ast.tree.spi.predicate.RelationalPredicate;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcMutation;
import org.hibernate.sql.exec.spi.ParameterBindingContext;

public class RxSingleTableEntityTypeDescriptor<T> extends SingleTableEntityTypeDescriptor<T> implements
		EntityTypeDescriptor<T> {

	public RxSingleTableEntityTypeDescriptor(
			EntityMapping bootMapping,
			IdentifiableTypeDescriptor superTypeDescriptor,
			RuntimeModelCreationContext creationContext)
			throws HibernateException {
		super( bootMapping, superTypeDescriptor, creationContext );
	}

	@Override
	public RxSingleIdEntityLoader getSingleIdLoader() {
		return new RxSingleIdEntityLoader( this );
	}

	public void delete(
			Object id,
			Object version,
			Object object,
			SharedSessionContractImplementor session,
			CompletionStage<Void> stage)
			throws HibernateException {

		// todo (6.0) - initial basic pass at entity deletes

		final Object unresolvedId = getHierarchy().getIdentifierDescriptor().unresolve( id, session );
		final ExecutionContext executionContext = getExecutionContext( session );


//		deleteSecondaryTables( session, unresolvedId, executionContext );

		deleteRootTable( session, unresolvedId, executionContext, stage );
	}


	private void deleteRootTable(
			SharedSessionContractImplementor session,
			Object unresolvedId,
			ExecutionContext executionContext,
			CompletionStage<Void> deleteStage) {
		final TableReference tableReference = new TableReference( getPrimaryTable(), null, false );

		final Junction identifierJunction = new Junction( Junction.Nature.CONJUNCTION );
		getHierarchy().getIdentifierDescriptor().dehydrate(
				unresolvedId,
				(jdbcValue, type, boundColumn) ->
						identifierJunction.add(
								new RelationalPredicate(
										RelationalPredicate.Operator.EQUAL,
										new ColumnReference( boundColumn ),
										new LiteralParameter(
												jdbcValue,
												boundColumn.getExpressableType(),
												Clause.DELETE,
												session.getFactory().getTypeConfiguration()
										)
								)
						)
				,
				Clause.DELETE,
				session
		);

		executeDelete( executionContext, tableReference, identifierJunction, deleteStage );
	}


	private void executeDelete(
			ExecutionContext executionContext,
			TableReference tableReference,
			Junction identifierJunction,
			CompletionStage<Void> deleteStage) {
		final DeleteStatement deleteStatement = new DeleteStatement( tableReference, identifierJunction );

		final JdbcMutation delete = SqlDeleteToJdbcDeleteConverter.interpret(
				new SqlAstDeleteDescriptor() {
					@Override
					public DeleteStatement getSqlAstStatement() {
						return deleteStatement;
					}

					@Override
					public Set<String> getAffectedTableNames() {
						return Collections.singleton(
								deleteStatement.getTargetTable().getTable().getTableExpression()
						);
					}
				},
				executionContext.getSession().getSessionFactory()
		);

		executeOperation( executionContext, deleteStatement, deleteStage );
	}

	@Override
	public void insert(
			Object id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		EntityEntry entry = session.getPersistenceContext().getEntry( object );
		super.insert( id, fields, object, session );
	}

	@Override
	protected Object insertInternal(
			Object id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session) {
		EntityEntry entry = session.getPersistenceContext().getEntry( object );
		CompletionStageExtraState extraState = entry.getExtraState( CompletionStageExtraState.class );
		return insertInternal( id, fields, object, session, (CompletionStage<Void>) extraState.getStage() );
	}

	protected Object insertInternal(
			Object id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session,
			CompletionStage<Void> insertStage) {

		// generate id if needed
//		if ( id == null ) {
//			final IdentifierGenerator generator = getHierarchy().getIdentifierDescriptor().getIdentifierValueGenerator();
//			if ( generator != null ) {
//				id = generator.generate( session, object );
//			}
//		}

//		final Object unresolvedId = getHierarchy().getIdentifierDescriptor().unresolve( id, session );
		final Object unresolvedId = id;
		final ExecutionContext executionContext = getExecutionContext( session );

		// for now - just root table
		// for now - we also regenerate these SQL AST objects each time - we can cache these
		executeInsert( fields, session, unresolvedId, executionContext, new TableReference( getPrimaryTable(), null, false), insertStage );

		return id;
	}

	private ExecutionContext getExecutionContext(SharedSessionContractImplementor session) {
		return new ExecutionContext() {
			private final ParameterBindingContext parameterBindingContext = new TemplateParameterBindingContext( session.getFactory() );

			@Override
			public SharedSessionContractImplementor getSession() {
				return session;
			}

			@Override
			public QueryOptions getQueryOptions() {
				return new QueryOptionsImpl();
			}

			@Override
			public ParameterBindingContext getParameterBindingContext() {
				return parameterBindingContext;
			}

			@Override
			public Callback getCallback() {
				return afterLoadAction -> {
				};
			}
		};
	}

	private void executeInsert(
			Object[] fields,
			SharedSessionContractImplementor session,
			Object unresolvedId,
			ExecutionContext executionContext,
			TableReference tableReference,
			CompletionStage<?> stage) {

		final InsertStatement insertStatement = new InsertStatement( tableReference );
		// todo (6.0) : account for non-generated identifiers

		getHierarchy().getIdentifierDescriptor().dehydrate(
				// NOTE : at least according to the argument name (`unresolvedId`), the
				// 		incoming id value should already be unresolved - so do not
				// 		unresolve it again
				getHierarchy().getIdentifierDescriptor().unresolve( unresolvedId, session ),
				//unresolvedId,
				(jdbcValue, type, boundColumn) -> {
					insertStatement.addTargetColumnReference( new ColumnReference( boundColumn ) );
					insertStatement.addValue(
							new LiteralParameter(
									jdbcValue,
									boundColumn.getExpressableType(),
									Clause.INSERT,
									session.getFactory().getTypeConfiguration()
							)
					);
				},
				Clause.INSERT,
				session
		);

		visitStateArrayContributors(
				contributor -> {
					final int position = contributor.getStateArrayPosition();
					final Object domainValue = fields[position];
					contributor.dehydrate(
							// todo (6.0) : fix this - specifically this isInstance check is bad
							// 		sometimes the values here are unresolved and sometimes not;
							//		need a way to ensure they are always one form or the other
							//		during these calls (ideally unresolved)
							contributor.getJavaTypeDescriptor().isInstance( domainValue )
									? contributor.unresolve( domainValue, session )
									: domainValue,
							(jdbcValue, type, boundColumn) -> {
								if ( boundColumn.getSourceTable().equals( tableReference.getTable() ) ) {
									addInsertColumn( session, insertStatement, jdbcValue, boundColumn, type );
								}
							},
							Clause.INSERT,
							session
					);
				}
		);

		executeOperation( executionContext, insertStatement, stage );
	}

	private void executeOperation(ExecutionContext executionContext, InsertStatement insertStatement, CompletionStage<?> operationStage) {
		RxOperation mutation = SqlInsertToRxInsertConverter.createRxInsert( insertStatement, executionContext.getSession().getSessionFactory() );
		RxMutationExecutor executor = new RxMutationExecutor();
		executor.execute( mutation, executionContext, operationStage );
	}


	private void executeOperation(ExecutionContext executionContext, DeleteStatement deleteStatement, CompletionStage<?> operationStage) {
		RxOperation mutation = SqlDeleteToRxDeleteConverter.createRxDelete( deleteStatement, executionContext.getSession().getSessionFactory() );
		RxMutationExecutor executor = new RxMutationExecutor();
		executor.execute( mutation, executionContext, operationStage );
	}

	private void addInsertColumn(
			SharedSessionContractImplementor session,
			InsertStatement insertStatement,
			Object jdbcValue,
			Column referringColumn,
			SqlExpressableType expressableType) {
		if ( jdbcValue != null ) {
			insertStatement.addTargetColumnReference( new ColumnReference( referringColumn ) );
			insertStatement.addValue(
					new LiteralParameter(
							jdbcValue,
							expressableType,
							Clause.INSERT,
							session.getFactory().getTypeConfiguration()
					)
			);
		}
	}

	private class ValuesNullChecker {
		private boolean allNull = true;

		private void setNotAllNull(){
			allNull = false;
		}

		public boolean areAllNull(){
			return allNull;
		}
	}
}
