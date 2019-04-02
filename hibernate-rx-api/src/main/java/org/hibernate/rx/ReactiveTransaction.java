package org.hibernate.rx;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface ReactiveTransaction {

	<T> CompletionStage<T> runAsync(Function<RxSession, CompletionStage<T>> consumer);

	void commit();
}
