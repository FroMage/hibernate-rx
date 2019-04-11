/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.exec.internal;

import java.util.List;
import java.util.Set;

import org.hibernate.rx.sql.ast.consume.spi.PgResultMappingDescriptor;
import org.hibernate.rx.sql.ast.consume.spi.RxParameterBinder;
import org.hibernate.rx.sql.ast.consume.spi.RxSelect;
import org.hibernate.sql.results.spi.ResultSetMappingDescriptor;

public class RxSelectImpl implements RxSelect {
	private final String sql;
	private final List<RxParameterBinder> parameterBinders;
	private final PgResultMappingDescriptor resultDescriptor;
	private final Set<String> affectedTableNames;

	public RxSelectImpl(
			String sql,
			List<RxParameterBinder> parameterBinders,
			PgResultMappingDescriptor resultDescriptor,
			Set<String> affectedTableNames) {
		this.sql = sql;
		this.parameterBinders = parameterBinders;
		this.resultDescriptor = resultDescriptor;
		this.affectedTableNames = affectedTableNames;
	}

	@Override
	public String getSql() {
		return sql;
	}

	@Override
	public List<RxParameterBinder> getParameterBinders() {
		return parameterBinders;
	}


	@Override
	public Set<String> getAffectedTableNames() {
		return affectedTableNames;
	}

	@Override
	public PgResultMappingDescriptor getPgResultMappingDescriptor() {
		return null;
	}
}
