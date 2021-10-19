package com.mercadolibre.flowbacklogchecker.consolidation;

public interface TransitionEvent {

	String getEntityId();

	EntityState getNewState();

	EntityState getOldState();
}