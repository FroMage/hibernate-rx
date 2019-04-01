package org.hibernate.rx;

public interface ReactiveTransactionWork<T> {
	T execute(ReactiveTransaction tx);
}
