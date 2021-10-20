package com.mercadolibre.flowbacklogchecker.consolidation;


import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An {@link EventsSource} that pushes the events stored in the "incoming_events" table.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StoredEventsSource implements EventsSource {

	//	private final JdbcTemplate template;
	private final Connection connection;

	@SneakyThrows
	public void provideWhile(
			final long startingEventSerialNumberExclusive,
			final ContextPredicate whileCondition,
			final Sink sink
	) {
		long lastProvidedSerial = startingEventSerialNumberExclusive;
		long pageStartingSerial;
		do {
			pageStartingSerial = lastProvidedSerial;
			var ps = connection.prepareStatement("SELECT id, entity_id, entity_type, struct_version, new_state, old_state "
					+ "FROM incoming_events "
					+ "WHERE id > ? "
					+ "ORDER BY id ASC "
					+ "LIMIT 10000", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
			ps.setLong(1, lastProvidedSerial);
			var rs = ps.executeQuery();
			while (whileCondition.test() && rs.next()) {
				var er = EventRecord.fromResultSet(rs);
				lastProvidedSerial = er.arrivalSerialNumber;
				sink.accept(er);
			}
			log.info("The events whose arrival serial is between {} and {} where provided", pageStartingSerial, lastProvidedSerial);

		} while (lastProvidedSerial > pageStartingSerial);

		//////

//		template.query(
//				"SELECT id, entity_id, entity_type, struct_version, new_state, old_state "
//						+ "FROM incoming_events "
//						+ "WHERE id > ? "
//						+ "ORDER BY id",
//				ps -> ps.setLong(1, startingEventSerialNumberExclusive),
//				rs -> {
//					while (whileCondition.test() && rs.next()) {
//						sink.accept(EventRecord.fromResultSet(rs));
//					}
//					return null;
//				}
//		);
	}
}