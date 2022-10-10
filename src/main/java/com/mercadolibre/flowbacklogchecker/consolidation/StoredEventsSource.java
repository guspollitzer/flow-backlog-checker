package com.mercadolibre.flowbacklogchecker.consolidation;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An {@link EventsSource} that pushes the events stored in the "incoming_events" table.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StoredEventsSource implements EventsSource {

	private static final int PAGE_SIZE = 100_000;

	private final Connection connection;

	@Override
	public void provideWhile(final long startingEventArrivalSerialNumberExclusive, final Sink sink) throws SQLException {

		final var state = new State(startingEventArrivalSerialNumberExclusive);
		do {
			state.recordsRead = 0;
			executeQuery(sink, state);
		} while (state.keepGoing && state.recordsRead == PAGE_SIZE);
	}

	private void executeQuery(final Sink sink, final State state) throws SQLException {
		var ps = connection.prepareStatement(
				"SELECT event_id, id, date_created, entity_id, entity_type, struct_version, new_state, old_state "
						+ "FROM incoming_events "
						+ "WHERE id > ? AND entity_type = 'outbound-unit' AND new_state like '%\"warehouse_id\":\"BRSP03\"%' "
						+ "ORDER BY id "
						+ "LIMIT ?"
		);
		ps.setLong(1, state.lastEventRead);
		ps.setInt(2, PAGE_SIZE);
		ps.setFetchSize(Integer.MIN_VALUE);

		final long pageStartingSerial = state.lastEventRead;

		var rs = ps.executeQuery();
		while (state.keepGoing && rs.next()) {
			state.recordsRead++;
			state.lastEventRead = rs.getLong("id");
			state.keepGoing = sink.accept(EventRecord.fromResultSet(rs));
		}

		log.info("The events whose arrival serial is between {} and {} where provided", pageStartingSerial, state.lastEventRead);

	}

	private static class State {
		private boolean keepGoing;
		private long recordsRead;
		private long lastEventRead;

		State(final long lastEventRead) {
			keepGoing = true;
			recordsRead = 0;
			this.lastEventRead = lastEventRead;
		}
	}
}