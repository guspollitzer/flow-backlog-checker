package com.mercadolibre.flowbacklogchecker.consolidation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@Slf4j
public class Boot {

//	public final DataSource dataSource;
//	public final JdbcTemplate jdbcTemplate;
	public PartitionsCatalog partitionsCatalog;
	public EventRecordParser eventRecordParser;
	public StoredEventsSource storedEventsSource;

	@SneakyThrows
	public Boot() {
		var url = "jdbc:mysql://proxysql.slave.meliseginf.com:6612/backlogprd?useUnicode=yes&characterEncoding=UTF-8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";
		var connection = DriverManager.getConnection(url, System.getenv("DB_USER"), System.getenv("DB_PASSWORD"));
//		dataSource = DataSourceBuilder.create()
//				.username(System.getenv("DB_USER"))
//				.password(System.getenv("DB_PASSWORD"))
//				.driverClassName("com.mysql.cj.jdbc.Driver")
//				.url(url)
//				.build();
//		jdbcTemplate = new JdbcTemplate(dataSource);

		partitionsCatalog = new PartitionsCatalog();
		eventRecordParser = new EventRecordParser(objectMapper());
		storedEventsSource = new StoredEventsSource(connection);
	}

	public void start(final long startingArrivalSerialNumber) {
		log.info("Starting...");
		Backlog backlog;
		try {
			backlog = new Backlog(partitionsCatalog, eventRecordParser, startingArrivalSerialNumber, null);
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
