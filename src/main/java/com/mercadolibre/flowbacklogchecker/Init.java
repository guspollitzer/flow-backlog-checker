package com.mercadolibre.flowbacklogchecker;

import com.mercadolibre.flowbacklogchecker.consolidation.Backlog;
import com.mercadolibre.flowbacklogchecker.consolidation.EventRecordParser;
import com.mercadolibre.flowbacklogchecker.consolidation.EventsSource;
import com.mercadolibre.flowbacklogchecker.consolidation.PartitionsCatalog;
import com.mercadolibre.flowbacklogchecker.consolidation.StoredEventsSource;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.io.IOException;

@RequiredArgsConstructor
@Component()
@DependsOn({"mysqlDataSource"})
public class Init {

	private final static Logger log = LoggerFactory.getLogger(Init.class);

	private final PartitionsCatalog partitionsCatalog;
	private final EventRecordParser eventRecordParser;
	private final StoredEventsSource storedEventsSource;

	@PostConstruct
	void init() {
		log.info("Starting...");
		Backlog backlog;
		try {
			backlog = new Backlog(partitionsCatalog, eventRecordParser, 1, null);
			final long serialNumberOfLastEventOfLastPhoto = backlog.getLastEventArrivalSerialNumber();
			storedEventsSource.provideWhile(
					serialNumberOfLastEventOfLastPhoto,
					() -> true,
					eventConsolidator(backlog, serialNumberOfLastEventOfLastPhoto)
			);

		} finally {
			log.info("Finishing...");
		}
	}

	private EventsSource.Sink eventConsolidator(final Backlog backlog, final long serialNumberOfLastEventOfLastPhoto) {
		return eventRecord -> {
			try {
				backlog.merge(eventRecord);
			} catch (IOException | EventRecordParser.NotSupportedStructureVersion e) {
				final String message = String.format(
						"The incoming event with arrival serial number %d was discarded because the conversion form "
								+ "EventRecord to TransitionEvent has failed. Therefore, the photo under construction may be "
								+ "corrupted. The last serial number of the las event of the last photo not affected by this "
								+ "problem is %d.",
						eventRecord.getArrivalSerialNumber(), serialNumberOfLastEventOfLastPhoto
				);
				log.error(message, e);
				// TODO Trigger an alarm.
			}
		};
	}
}
