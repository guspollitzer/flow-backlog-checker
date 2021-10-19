package com.mercadolibre.flowbacklogchecker.consolidation;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Counts how many entities are in each of the discriminated state subsets, for all entities whose state transition events were merged into this
 * backlog.
 */
public class Backlog {
	private static final int CELLS_HASH_MAP_INITIAL_CAPACITY = 8192;

	private static final int LAST_MERGED_EVENTS_HASH_MAP_INITIAL_CAPACITY = 8192;

	private final PartitionsCatalog partitionsCatalog;

	private final EventRecordParser eventRecordParser;

	/**
	 * The date of the backlog photo this backlog was initialized with.
	 */
	private final Timestamp initialPhotoWasTakenOn;

	/**
	 * The arrival serial number of the last event that was merged into this backlog.
	 */
	private long lastEventArrivalSerialNumber;

	private final Map<Index, Cell> cells;

	public Backlog(
			final PartitionsCatalog partitionsCatalog,
			final EventRecordParser eventRecordParser,
			final long lastEventArrivalSerialNumber,
			final Timestamp initialPhotoWasTakenOn
	) {
		this.partitionsCatalog = partitionsCatalog;
		this.eventRecordParser = eventRecordParser;
		this.lastEventArrivalSerialNumber = lastEventArrivalSerialNumber;
		this.initialPhotoWasTakenOn = initialPhotoWasTakenOn;

		this.cells = new HashMap<>(CELLS_HASH_MAP_INITIAL_CAPACITY);
	}

	public Timestamp getInitialPhotoWasTakenOn() {
		return this.initialPhotoWasTakenOn;
	}

	/**
	 * Tells how many cells were traversed by an entity state.
	 */
	public int getNumberOfCellsTraversed() {
		return cells.size();
	}

	/**
	 * Gives the arrival serial number of the last merged event.
	 */
	public long getLastEventArrivalSerialNumber() {
		return this.lastEventArrivalSerialNumber;
	}

	/**
	 * Updates this {@link Backlog} decrementing the population of the cell that contains the old state ({@link TransitionEvent#getOldState()}), and
	 * incrementing the population of the cell that contains the new state ({@link TransitionEvent#getNewState()}).
	 *
	 * <p>Duplicate events are ignored.
	 */
	public void merge(final EventRecord eventRecord) throws EventRecordParser.NotSupportedStructureVersion, IOException {
		assert (eventRecord.arrivalSerialNumber > this.lastEventArrivalSerialNumber);
		this.lastEventArrivalSerialNumber = eventRecord.arrivalSerialNumber;
		// ignore consecutive copies of the same event for each entity.

		final TransitionEvent transitionEvent = eventRecordParser.parse(eventRecord);

		final EntityState oldState = transitionEvent.getOldState();
		if (oldState != null) {
			final Index fromIndex = indexOf(oldState);
			this.getCell(fromIndex).decrement(transitionEvent.getEntityId());
		}

		final EntityState newState = transitionEvent.getNewState();
		if (newState != null) {
			final Index toIndex = indexOf(transitionEvent.getNewState());
			this.getCell(toIndex).increment(transitionEvent.getEntityId());
		}
	}

	public void loadCell(Object[] indexValues, int population) {
		this.cells.put(new Index(indexValues), new Cell(population));
	}

	/**
	 * Gives a list containing all the index-cell entries whose population or variation is not zero.
	 */
	public List<IndexCellEntry> getIndexCellEntries() {
		return this.cells.entrySet().stream()
				.filter(e -> e.getValue().population != 0 || e.getValue().variation != 0)
				.map(e -> new IndexCellEntry(
						e.getKey().indexValues,
						e.getValue().population,
						e.getValue().variation
				)).collect(Collectors.toList());
	}

	/**
	 * Finds out the {@link Index} of the {@link Cell} that contains the specified {@link EntityState}.
	 */
	private Index indexOf(EntityState ite) {
		final var partitionParts = new Object[partitionsCatalog.getPartitions().size()];
		for (Partition partition : partitionsCatalog.getPartitions()) {
			partitionParts[partition.getOrdinal()] = partition.discriminator().apply(ite);
		}
		return new Index(partitionParts);
	}

	/**
	 * Gives the {@link Cell} pointed by the specified {@link Index}
	 */
	private Cell getCell(Index index) {
		Cell cell = this.cells.get(index);
		if (cell == null) {
			cell = new Cell();
			this.cells.put(index, cell);
		}
		return cell;
	}

	@Getter
	@RequiredArgsConstructor
	public static class IndexCellEntry {
		final Object[] indexValues;

		final int population;

		final int variation;
	}

	/**
	 * The cells are the little parts of the entity state space in which the whole entity state space was divided by the known {@link Partition}s.
	 * Each cell knows how many entities have their state inside it.
	 */
	@NoArgsConstructor
	private static class Cell {
		private Set<String> entities = new HashSet<>();
		private final Set<String> addedWhenAlreadyPresent = new HashSet<>();
		private final Set<String> removedWhenAbsent = new HashSet<>();
		private int population;
		private int variation;

		private Cell(int population) {
			this.population = population;
		}

		private void increment(String entityId) {
			this.population += 1;
			this.variation += 1;
			var wasAbsent = this.entities.add(entityId);
			if (!wasAbsent) { addedWhenAlreadyPresent.add(entityId); }
		}

		private void decrement(String entityId) {
			this.population -= 1;
			this.variation -= 1;
			var wasPresent = this.entities.remove(entityId);
			if (!wasPresent) { removedWhenAbsent.add(entityId); }
		}
	}

	/**
	 * A multidimensional index. Instances of this class identify a {@link Cell} of the entity's discrete state space.
	 */
	private static class Index {
		private final Object[] indexValues;

		private final transient int hash;

		private Index(Object... indexValues) {
			this.indexValues = indexValues;
			this.hash = Arrays.hashCode(indexValues);
		}

		public boolean equals(final Object o) {
			return o instanceof Index && Arrays.equals(this.indexValues, ((Index) o).indexValues);
		}

		public int hashCode() {
			return hash;
		}
	}
}
