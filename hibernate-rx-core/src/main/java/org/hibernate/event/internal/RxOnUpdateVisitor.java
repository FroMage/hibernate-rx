package org.hibernate.event.internal;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.EventSource;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;

public class RxOnUpdateVisitor extends OnUpdateVisitor {

	public RxOnUpdateVisitor(EventSource session, Object key, Object owner) {
		super( session, key, owner );
	}

	@Override
	public void process(Object object, EntityTypeDescriptor entityDescriptor) throws HibernateException {
		super.process( object, entityDescriptor );
	}
}
