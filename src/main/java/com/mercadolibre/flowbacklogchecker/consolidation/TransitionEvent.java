package com.mercadolibre.flowbacklogchecker.consolidation;

public interface TransitionEvent {

	long getEventId();

	long getArrivalSerialNumber();

	EntityState getNewState();

	EntityState getOldState();
}
