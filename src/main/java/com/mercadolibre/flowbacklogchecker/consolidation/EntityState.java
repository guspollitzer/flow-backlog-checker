package com.mercadolibre.flowbacklogchecker.consolidation;

import java.sql.Timestamp;

/**
 * Knows the state of a monitored entity.
 */
public interface EntityState {

	String getLogisticCenter();

	String getWorkflow();

	String getStatus();

	String getArea();

	Timestamp getDateIn();

	Timestamp getDeadline();

	/**
	 * Tells if this state is the last one: the entity will remain in this state for the rest of
	 * eternity.
	 *
	 * @return true if this state is the last one.
	 */
	boolean isUltimate();
}
