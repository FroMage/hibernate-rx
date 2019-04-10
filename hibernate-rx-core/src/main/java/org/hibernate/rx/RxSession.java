package org.hibernate.rx;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.rx.engine.spi.RxActionQueue;

public interface RxSession extends Session {

	@Override
	RxHibernateSessionFactory getSessionFactory();

	CompletionStage<Void> persistAsync(Object entity);

	CompletionStage<Void> deleteAsync(Object object);

	CompletionStage<Void> deleteAsync(String entityName, Object object);

	<T> CompletionStage<Optional<T>> findAsync(Class<T> type, Object id);

	<R> RxQuery<R> createQuery(Class<R> resultType, String jpql);

	RxActionQueue getRxActionQueue();

	CompletionStage<Void> removeAsync(Object entity);

	// Maybe this should be more an execute or run method
	Executor getExecutor();
}
