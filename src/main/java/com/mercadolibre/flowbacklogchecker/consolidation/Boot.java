package com.mercadolibre.flowbacklogchecker.consolidation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@Slf4j
public class Boot {
	private static final String URL = "jdbc:mysql://proxysql.slave.meliseginf.com:6612/backlogprd?useUnicode=yes&characterEncoding=UTF-8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";

	public PartitionsCatalog partitionsCatalog;
	public EventRecordParser eventRecordParser;

	public Boot() {
		partitionsCatalog = new PartitionsCatalog();
		eventRecordParser = new EventRecordParser(objectMapper());
	}

	public void start(final long startingArrivalSerialNumber) {
		Backlog backlog = new Backlog(partitionsCatalog, startingArrivalSerialNumber, null);
		log.info("Connecting...");
		try {
			while (true) {
				try (var connection = DriverManager.getConnection(URL, System.getenv("DB_USER"), System.getenv("DB_PASSWORD"))) {
					connection.setReadOnly(true);
					log.info("Connected");
					final StoredEventsSource storedEventsSource = new StoredEventsSource(connection);

					final long serialNumberOfLastEventOfLastPhoto = backlog.getLastEventArrivalSerialNumber();
					storedEventsSource.provideWhile(
							serialNumberOfLastEventOfLastPhoto,
							() -> true,
							buildEventIntegrator(backlog, serialNumberOfLastEventOfLastPhoto)
					);

				} catch (SQLException sqlException) {
					log.error("Connection lost :", sqlException);
					Thread.sleep(10000);
					log.info("Reconnecting...");
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** Builds a pure effect procedure that integrates the event it receives into the specified backlog. */
	private EventsSource.Sink buildEventIntegrator(final Backlog backlog, final long serialNumberOfLastEventOfLastPhoto) {
		return eventRecord -> {
			try {
				final TransitionEvent transitionEvent = eventRecordParser.parse(eventRecord);
				backlog.merge(transitionEvent);
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


	public ObjectMapper objectMapper() {
		final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

		final ObjectMapper objectMapper = new ObjectMapper()
				.registerModule(new ParameterNamesModule())
				.registerModule(new Jdk8Module())
				.registerModule(new JavaTimeModule())
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
				.setSerializationInclusion(JsonInclude.Include.NON_NULL)
				.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		objectMapper.setDateFormat(sdf);
		return objectMapper;
	}
}
