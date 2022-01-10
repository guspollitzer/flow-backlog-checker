package com.mercadolibre.flowbacklogchecker.consolidation;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EventRecordParser {
	private final ObjectMapper objectMapper;

	public TransitionEvent parse(final EventRecord eventRecord) throws IOException, NotSupportedStructureVersion {
		Class<? extends EntityState> structure = EntityType.determineStructure(
				eventRecord.entityType,
				eventRecord.structVersion);

		return new TransitionEventImpl(
				eventRecord.eventId,
				eventRecord.arrivalSerialNumber,
				objectMapper.readValue(eventRecord.newStateRawJson, structure),
				objectMapper.readValue(eventRecord.oldStateRawJson, structure)
		);
	}

	@Getter
	@RequiredArgsConstructor
	public static class TransitionEventImpl implements TransitionEvent {
		public final long eventId;

		public final long arrivalSerialNumber;

		public final EntityState newState;

		public final EntityState oldState;
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
