package org.hibernate.rx;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.persistence.EntityTransaction;

import org.hibernate.Session;

public interface RxSession extends Session {

	@Override
	RxHibernateSessionFactory getSessionFactory();

	CompletionStage<Void> inTransaction(final Function<EntityTransaction, CompletionStage<Void>> consumer);

	CompletionStage<Void> persistAsync(Object entity);

	CompletionStage<Void> deleteAsync(Object object);

	CompletionStage<Void> deleteAsync(String entityName, Object object);

	<T> CompletionStage<Optional<T>> findAsync(Class<T> type, Object id);

	<R> RxQuery<R> createQuery(Class<R> resultType, String jpql);

	CompletionStage<Void> removeAsync(Object entity);

	// Maybe this should be more an execute or run method
	Executor getExecutor();
}
