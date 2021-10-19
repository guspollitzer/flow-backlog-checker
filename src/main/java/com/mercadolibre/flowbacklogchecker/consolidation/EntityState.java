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

	Timestamp getDeadline();
}