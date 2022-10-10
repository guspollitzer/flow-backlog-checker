package com.mercadolibre.flowbacklogchecker.consolidation;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Contains the known entity types and different versions of their JSON structures.
 */
public enum EntityType {
	outboundUnit("outbound-unit",
			new VersionedStructure(1, OutboundUnitStateV0.class));

	private static final Timestamp BIG_BANG = new Timestamp(0);

	private static final Map<String, EntityType> ENTITY_TYPE_MAP =
			Arrays.stream(EntityType.values())
					.collect(Collectors.toUnmodifiableMap(entityType -> entityType.id, Function.identity()));

	/** The identification of entity type as received from the incoming events. */
	public final String id;

	/**
	 * List of the {@link VersionedStructure}s corresponding to this {@link EntityType} instance, in
	 * descending order of the {@link VersionedStructure#startingVersion} field
	 */
	private final VersionedStructure[] versionedStructures;

	EntityType(String id, VersionedStructure... versionedStructures) {
		this.id = id;
		this.versionedStructures = Arrays.copyOf(versionedStructures, versionedStructures.length);
		// sort the elements in descending order
		Arrays.sort(this.versionedStructures, Comparator.comparingInt(sv -> -sv.startingVersion));
	}

	/**
	 * Finds the {@link VersionedStructure} corresponding to the specified entity type with the
	 * greatest {@link VersionedStructure#startingVersion} that is less than or equal to the specified
	 * version. Assumes that the {@link #versionedStructures} elements are in descending order.
	 *
	 * @param entityTypeName the name of the entity type
	 * @param version the version of the state structure
	 * @return the java class that contains the entity state
	 * @throws EventRecordParser.NotSupportedStructureVersion when the version is illegal
	 */
	public static Class<? extends EntityState> determineStructure(String entityTypeName, int version) throws EventRecordParser.NotSupportedStructureVersion {
		EntityType entityType = ENTITY_TYPE_MAP.get(entityTypeName);
		if (entityType != null) {
			for (VersionedStructure versionedStructure : entityType.versionedStructures) {
				if (versionedStructure.startingVersion <= version) {
					return  versionedStructure.backlogStructure;
				}
			}
		}
		throw new EventRecordParser.NotSupportedStructureVersion(entityTypeName, version);
	}

	@RequiredArgsConstructor
	private static class VersionedStructure {
		/**
		 * Incoming events whose struct version field is between this number inclusive and the
		 * startingVersion of the
		 */
		final int startingVersion;

		/**
		 * A java class that matches the JSON structure of the entity state.
		 */
		final Class<? extends EntityState> backlogStructure;


	}

	@Getter
	@NoArgsConstructor
	@EqualsAndHashCode
	public static class OutboundUnitStateV0 implements EntityState {
		private static Timestamp lastDateCreated = new Timestamp(0);


		private String logisticCenter;

		private String workflow;

		private String status;

		private Timestamp dateIn;

		private String area;

		private Timestamp deadline;

		private boolean ultimate;

		public void setWarehouseId(String warehouseId) {this.logisticCenter = warehouseId;}

		public void setGroupType(String groupType) {this.workflow = groupType;}

		public void setStatus(String status) {this.status = status;}

		public void setDateCreated(Timestamp dateCreated) {
			if (dateCreated != null) {
				this.dateIn = Timestamp.from(dateCreated.toInstant().truncatedTo(HOURS));

				if (dateCreated.after(lastDateCreated)) {
					lastDateCreated = dateCreated;
				}
			} else if ("PENDING".equals(status)){
				this.dateIn = Timestamp.from(lastDateCreated.toInstant().truncatedTo(HOURS));;
			} else {
				this.dateIn = BIG_BANG;
			}
		}

		public void setStorageId(String storageId) {
			final String[] addressFields = storageId != null ? storageId.split("-") : null;
			this.area = addressFields != null && addressFields.length > 1 ? addressFields[0] : null;
		}

		public void setEstimatedTimeDeparture(Timestamp estimatedTimeDeparture) {
			this.deadline = Timestamp.from(estimatedTimeDeparture.toInstant().truncatedTo(SECONDS));
		}

		public void setUltimate(boolean ultimate) {this.ultimate = ultimate || "OUT".equals(status);}
	}
}
