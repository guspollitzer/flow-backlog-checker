package com.mercadolibre.flowbacklogchecker.consolidation;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import static java.time.temporal.ChronoUnit.HOURS;

/**
 * Contains the known entity types and different versions of their JSON structures.
 */
public enum EntityType {
	inboundUnit("INBOUND_UNIT", new VersionedStructure(1, InboundUnitStateV1.class));

	private static final Map<String, EntityType> ENTITY_TYPE_MAP = Arrays.stream(EntityType.values()).collect(
			Collectors.toUnmodifiableMap(entityType -> entityType.id, Function.identity())
	);

	/**
	 * The identification of entity type as received from the incoming events.
	 */
	public final String id;

	/**
	 * List of the {@link VersionedStructure}s corresponding to this {@link EntityType} instance, in descending order of
	 * the {@link VersionedStructure#startingVersion} field
	 */
	private final VersionedStructure[] versionedStructures;

	EntityType(String id, VersionedStructure... versionedStructures) {
		this.id = id;
		this.versionedStructures = Arrays.copyOf(versionedStructures, versionedStructures.length);
		// sort the elements in descending order
		Arrays.sort(this.versionedStructures, Comparator.comparingInt(sv -> -sv.startingVersion));
	}

	/**
	 * Finds the {@link VersionedStructure} corresponding to the specified entity type with the greatest {@link
	 * VersionedStructure#startingVersion} that is less than or equal to the specified version. Assumes that the {@link
	 * #versionedStructures} elements are in descending order.
	 *
	 * @param entityTypeName the name of the entity type
	 * @param version the version of the state structure
	 * @return the java class that contains the entity state
	 * @throws EventRecordParser.NotSupportedStructureVersion when the version is illegal
	 */
	public static Class<? extends EntityState> determineStructure(String entityTypeName, int version)
			throws EventRecordParser.NotSupportedStructureVersion {
		EntityType entityType = ENTITY_TYPE_MAP.get(entityTypeName);
		if (entityType != null) {
			for (VersionedStructure versionedStructure : entityType.versionedStructures) {
				if (versionedStructure.startingVersion <= version) {
					return versionedStructure.structure;
				}
			}
		}
		throw new EventRecordParser.NotSupportedStructureVersion(entityTypeName, version);
	}

	@RequiredArgsConstructor
	private static class VersionedStructure {
		/**
		 * Incoming events whose struct version field is between this number inclusive and the startingVersion of the
		 */
		final int startingVersion;

		/**
		 * A java class that matches the JSON structure of the entity state.
		 */
		final Class<? extends EntityState> structure;
	}

	@Setter
	@NoArgsConstructor
	public static class InboundUnitStateV1 implements EntityState {
		private String eventType;

		private String inboundId;

		private String warehouseId;

		private String flowStep;

		private Timestamp appointmentDate;

		private Timestamp slaExpirationDate;

		private int unitQuantity;

		@Override
		public String getEventType() {
			return this.eventType;
		}

		@Override
		public String getEntityId() {
			return inboundId;
		}

		@Override
		public String getLogisticCenter() {
			return warehouseId;
		}

		@Override
		public String getWorkflow() {
			return "inbound";
		}

		@Override
		public String getStatus() {
			return flowStep;
		}

		@Override
		public String getArea() {
			return null;
		}

		@Override
		public Timestamp getDateIn() {
			boolean condition = appointmentDate == null || !"SCHEDULED".equals(flowStep);
			return condition ? new Timestamp(0) : Timestamp.from(appointmentDate.toInstant().truncatedTo(HOURS));
		}

		@Override
		public Timestamp getDeadline() {
			return slaExpirationDate == null ? new Timestamp(0) : Timestamp.from(slaExpirationDate.toInstant().truncatedTo(HOURS));
		}

		@Override
		public boolean isUltimate() {
			return false;
		}

		@Override
		public int getQuantity() {
			return unitQuantity;
		}
	}
}
