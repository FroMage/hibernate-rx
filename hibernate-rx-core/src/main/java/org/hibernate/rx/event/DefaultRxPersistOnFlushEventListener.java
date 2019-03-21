package org.hibernate.rx.event;

import org.hibernate.engine.spi.CascadingAction;
import org.hibernate.engine.spi.CascadingActions;

public class DefaultRxPersistOnFlushEventListener extends DefaultRxPersistEventListener {
	protected CascadingAction getCascadeAction() {
		return CascadingActions.PERSIST_ON_FLUSH;
	}
}
