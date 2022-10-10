package com.mercadolibre.flowbacklogchecker.consolidation;

import java.time.Instant;

public interface TransitionEvent {

	long getEventId();

	long getArrivalSerialNumber();

	long getEntityId();

	Instant getArrivalDate();

	EntityState getNewState();

	EntityState getOldState();
}
