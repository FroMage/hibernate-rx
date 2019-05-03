/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.transaction.internal;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorBuilderInitiator;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorBuilder;
import org.hibernate.rx.transaction.emulated.internal.EmulatedLocalTransactionCoordinatorBuilder;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * Contributes OGM's {@link TransactionCoordinatorBuilder}.
 * <p>
 * Makes use of ORM's default builder in the case of JTA, otherwise {@link EmulatedLocalTransactionCoordinatorBuilder}
 * is used.
 */
public class RxTransactionCoordinatorBuilderInitiator implements StandardServiceInitiator<TransactionCoordinatorBuilder> {

	public static final RxTransactionCoordinatorBuilderInitiator INSTANCE = new RxTransactionCoordinatorBuilderInitiator();

	private RxTransactionCoordinatorBuilderInitiator() {
	}

	@Override
	public Class<TransactionCoordinatorBuilder> getServiceInitiated() {
		return TransactionCoordinatorBuilder.class;
	}

	@Override
	public TransactionCoordinatorBuilder initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		TransactionCoordinatorBuilder builder = null;
		TransactionCoordinatorBuilder defaultBuilder = TransactionCoordinatorBuilderInitiator.INSTANCE.initiateService( configurationValues, registry );
		builder = new EmulatedLocalTransactionCoordinatorBuilder( defaultBuilder );
		return builder;
	}

	private TransactionCoordinatorBuilder getDefaultBuilder(ServiceRegistryImplementor registry, String strategy ) {
		return registry.getService( StrategySelector.class ).resolveStrategy( TransactionCoordinatorBuilder.class, strategy );
	}

}
