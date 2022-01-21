package com.mercadolibre.flowbacklogchecker.consolidation;

public interface TransitionEvent {

	long getEventId();

	long getArrivalSerialNumber();

	String getEntityId();

	EntityState getNewState();

	EntityState getOldState();
}