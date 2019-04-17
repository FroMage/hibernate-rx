/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.rx.sql.results.internal;

import java.sql.SQLException;

import org.hibernate.sql.results.internal.values.JdbcValues;
import org.hibernate.sql.results.spi.ResultSetMapping;
import org.hibernate.sql.results.spi.RowProcessingState;

/**
 * Provides unified access to query results (JDBC values - see
 * {@link RowProcessingState#getJdbcValue} whether they come from
 * query cache or ResultSet.  Implementations also manage any cache puts
 * if required.
 */
public interface RxValues extends JdbcValues {
}
