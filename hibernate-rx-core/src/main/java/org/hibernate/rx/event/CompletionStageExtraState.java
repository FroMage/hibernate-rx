package org.hibernate.rx.event;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.EntityEntryExtraState;

public class CompletionStageExtraState implements EntityEntryExtraState {

	private final CompletionStage<?> stage;

	public CompletionStageExtraState(CompletionStage<?> stage) {
		this.stage = stage;
	}

	public CompletionStage<?> getStage() {
		return stage;
	}

	@Override
	public void addExtraState(EntityEntryExtraState extraState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T extends EntityEntryExtraState> T getExtraState(Class<T> extraStateType) {
		throw new UnsupportedOperationException();
	}
}
