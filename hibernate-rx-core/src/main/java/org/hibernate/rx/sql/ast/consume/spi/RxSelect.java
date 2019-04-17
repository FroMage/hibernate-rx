package org.hibernate.rx.sql.ast.consume.spi;

import org.hibernate.sql.results.spi.ResultSetMappingDescriptor;

public interface RxSelect extends RxOperation {
	ResultSetMappingDescriptor getResultSetMappingDescriptor();
}
