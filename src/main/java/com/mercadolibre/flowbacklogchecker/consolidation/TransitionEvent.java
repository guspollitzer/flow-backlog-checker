package com.mercadolibre.flowbacklogchecker.consolidation;

public interface TransitionEvent {

	long getEventId();

	long getEntityId();

	long getArrivalSerialNumber();

	EntityState getNewState();

	EntityState getOldState();
}
