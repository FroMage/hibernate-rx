package org.hibernate.rx.impl;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.persistence.EntityTransaction;

import org.hibernate.rx.ReactiveTransaction;
import org.hibernate.rx.RxSession;

public class ReactiveTransactionImpl implements ReactiveTransaction {

	private RxSession session;
	private EntityTransaction tx;

	public ReactiveTransactionImpl(RxSession session, EntityTransaction tx) {
		this.session = session;
		this.tx = tx;
	}

	@Override
	public <T> CompletionStage<T> runAsync(Function<RxSession, CompletionStage<T> > consumer) {
		return consumer.apply( session );
	}

	@Override
	public void commit() {
		tx.commit();
	}

}
