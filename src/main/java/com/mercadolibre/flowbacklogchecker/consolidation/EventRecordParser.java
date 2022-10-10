package com.mercadolibre.flowbacklogchecker.consolidation;


import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EventRecordParser {
	private final ObjectMapper objectMapper;

	public TransitionEvent parse(final EventRecord eventRecord) throws IOException, NotSupportedStructureVersion {
		Class<? extends EntityState> structure =
				EntityType.determineStructure(eventRecord.entityType, eventRecord.structVersion);
		return new TransitionEventImpl(
				eventRecord.eventId,
				eventRecord.arrivalSerialNumber,
				eventRecord.arrivalDate,
				eventRecord.entityId,
				objectMapper.readValue(eventRecord.newStateRawJson, structure),
				objectMapper.readValue(eventRecord.oldStateRawJson, structure)
		);
	}

	@Getter
	@RequiredArgsConstructor
	public static class TransitionEventImpl implements TransitionEvent {
		public final long eventId;

		public final long arrivalSerialNumber;

		public final Instant arrivalDate;

		public final long entityId;

		public final EntityState newState;

		public final EntityState oldState;

		@Override
		public String toString() {
			var fields = Arrays.stream(PartitionsCatalog.PartitionsDb.values())
					.map(p -> {
						var oldVal = oldState != null ? p.valueGetter.apply(oldState) : null;
						var newVal = newState != null ? p.valueGetter.apply(newState) : null;
						return Objects.equals(newVal, oldVal) ? String.format("%-46s", newVal) : String.format("%-21s -> %-21s", oldVal, newVal);
					}).collect(Collectors.joining(", "));
			return String.format("%s -> {%s}", entityId, fields);
		}
	}

	public static class NotSupportedStructureVersion extends Exception {
		private static final long serialVersionUID = 238946890347L;

		NotSupportedStructureVersion(
				final String entityTypeName,
				final int structVersion
		) {
			super(String.format("entityType:%s, version:%d", entityTypeName, structVersion));
		}
	}
}
