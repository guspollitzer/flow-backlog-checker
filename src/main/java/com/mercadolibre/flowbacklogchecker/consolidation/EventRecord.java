package com.mercadolibre.flowbacklogchecker.consolidation;

import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * A transition event as received from the fury stream, tagged with a serial number that tells the order of arrival.
 */
@Getter
@RequiredArgsConstructor
@ToString
public class EventRecord {

	final long eventId;

	/**
	 * The sequence number generated and associated to this event after it arrived to this app when it was stored in the
	 * incoming events table.
	 */
	final long arrivalSerialNumber;

	final String entityId;

	final String entityType;

	/**
	 * The version of the structure of the {@link #newStateRawJson} and {@link #oldStateRawJson}
	 */
	final int structVersion;

	final String newStateRawJson;

	final String oldStateRawJson;

	public static EventRecord fromResultSet(final ResultSet rs) throws SQLException {
		return new EventRecord(
				rs.getLong("event_id"),
				rs.getLong("id"),
				rs.getString("entity_id"),
				rs.getString("entity_type"),
				rs.getInt("struct_version"),
				rs.getString("new_state"),
				rs.getString("old_state")
		);
	}
}
