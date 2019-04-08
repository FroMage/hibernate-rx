package org.hibernate.rx.impl;

import org.hibernate.rx.service.RxConnection;
import org.hibernate.service.UnknownUnwrapTypeException;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;

public class PgPoolConnection implements RxConnection {

	private final PgPoolOptions options;
	private final PgPool pool;

	public PgPoolConnection(PgPoolOptions options) {
		this.options = options;
		this.pool = PgClient.pool( options );
	}

	@Override
	public boolean isUnwrappableAs(Class unwrapType) {
		return PgPool.class.isAssignableFrom( unwrapType );
	}

	@Override
	public <T> T unwrap(Class<T> unwrapType) {
		if ( PgPool.class.isAssignableFrom( unwrapType ) ) {
			return (T) pool;
		}

		throw new UnknownUnwrapTypeException( unwrapType );
	}

}
