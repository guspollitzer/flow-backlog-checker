package com.mercadolibre.flowbacklogchecker.consolidation;


import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A transition event as received from the fury stream, tagged with a serial number that tells the order of arrival.
 */
@Getter
@RequiredArgsConstructor
public class EventRecord {
	/**
	 * The sequence number generated and associated to this event after it arrived to this app when it was stored in the
	 * incoming events table.
	 */
	final long arrivalSerialNumber;

	final String entityId;

	final String entityType;

	final int structVersion;

	final String newStateRawJson;

	final String oldStateRawJson;
}
