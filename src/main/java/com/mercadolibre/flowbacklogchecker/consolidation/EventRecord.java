package com.mercadolibre.flowbacklogchecker.consolidation;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A transition event as received from the fury stream, tagged with a serial number that tells the order of arrival.
 */
@Getter
@RequiredArgsConstructor
@ToString
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

	public static EventRecord fromResultSet(final ResultSet rs) throws SQLException {
		return new EventRecord(
				rs.getLong("id"),
				rs.getString("entity_id"),
				rs.getString("entity_type"),
				rs.getInt("struct_version"),
				rs.getString("new_state"),
				rs.getString("old_state")
		);
	}
}
