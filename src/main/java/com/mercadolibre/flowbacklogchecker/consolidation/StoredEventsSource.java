package com.mercadolibre.flowbacklogchecker.consolidation;


import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;

import static org.springframework.data.domain.Sort.Order.asc;
import static org.springframework.data.domain.Sort.by;
import static org.springframework.data.relational.core.query.Criteria.where;
import static org.springframework.data.relational.core.query.Query.query;

/**
 * An {@link EventsSource} that pushes the events stored in the "incoming_events" table.
 */
@Service
@RequiredArgsConstructor
public class StoredEventsSource implements EventsSource {

	private final R2dbcEntityTemplate template;

	public void provideWhile(
			final long startingEventSerialNumberExclusive,
			final ContextPredicate whileCondition,
			final Sink sink
	) {
		template.select(
				query(where("id").greaterThan(startingEventSerialNumberExclusive))
						.sort(by(asc("id"))),
				EventRecord.class
		).takeWhile(x -> whileCondition.test()).subscribe(sink::accept);
	}

}