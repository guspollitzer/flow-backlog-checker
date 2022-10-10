package com.mercadolibre.flowbacklogchecker.consolidation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A transition event as received from the fury stream, tagged with a serial number that tells the order of arrival.
 */
@Getter
@RequiredArgsConstructor
public class EventRecord {

	final long eventId;

	/**
	 * The sequence number generated and associated to this event after it arrived to this app when it was stored in the
	 * incoming events table.
	 */
	final long arrivalSerialNumber;

	final Instant arrivalDate;

	final long entityId;

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
				rs.getTimestamp("date_created").toInstant(),
				rs.getLong("entity_id"),
				rs.getString("entity_type"),
				rs.getInt("struct_version"),
				rs.getString("new_state"),
				rs.getString("old_state")
		);
	}
}
