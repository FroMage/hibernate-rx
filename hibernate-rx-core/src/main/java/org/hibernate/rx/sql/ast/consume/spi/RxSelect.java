package org.hibernate.rx.sql.ast.consume.spi;

import io.reactiverse.pgclient.PgResult;

public interface RxSelect extends RxOperation {
	PgResultMappingDescriptor getPgResultMappingDescriptor();
}
