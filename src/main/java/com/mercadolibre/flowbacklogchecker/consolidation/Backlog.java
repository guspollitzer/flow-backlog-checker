package com.mercadolibre.flowbacklogchecker.consolidation;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Counts how many entities are in each of the discriminated state subsets, for all entities whose state transition events were merged into this
 * backlog.
 */
@Slf4j
public class Backlog {
	private static final int CELLS_HASH_MAP_INITIAL_CAPACITY = 8192;

	private static final int LAST_MERGED_EVENTS_HASH_MAP_INITIAL_CAPACITY = 8192;

	public final PartitionsCatalog partitionsCatalog;

	public final EventRecordParser eventRecordParser;

	/**
	 * The date of the backlog photo this backlog was initialized with.
	 */
	public final Timestamp initialPhotoWasTakenOn;

	/**
	 * The arrival serial number of the last event that was merged into this backlog.
	 */
	public long lastEventArrivalSerialNumber;

	public final Map<Index, Cell> cells;

	public final Map<String, List<EventRecord>> trajectoriesByEntity;

	/** the amount of entities that where created */
	public int created = 0;
	/** the amount of entities that where destroyed */
	public int totalDestroyed = 0;
	/** the amount of entities that where destroyed after having been created */
	public int destroyedOfCreated = 0;
	/** the amount of entities that where destroyed but not created */
	public int destroyedOfNotCreated = 0;

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
		this.trajectoriesByEntity = new HashMap<>(65536);
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

		var trajectory = trajectoriesByEntity.computeIfAbsent(eventRecord.entityId, entityId -> new ArrayList<>(16));
		trajectory.add(eventRecord);

		final TransitionEvent transitionEvent = eventRecordParser.parse(eventRecord);

		final EntityState oldState = transitionEvent.getOldState();
		if (oldState != null) {
			final Index fromIndex = indexOf(oldState);
			this.getCell(fromIndex).decrement(eventRecord);
		} else {
			created += 1;
		}

		final EntityState newState = transitionEvent.getNewState();
		if (newState == null || "OUT".equals(newState.getStatus())) {
			if (trajectory.stream().anyMatch(er -> "null".equals(er.oldStateRawJson))) {
				destroyedOfCreated += 1;
				if (!checkTrajectory(trajectory)) {
					log.warn(
							"Irregular trajectory: {}",
							trajectory.stream().map(EventRecord::toString).collect(Collectors.joining("\n\t", "[\n\t", "]"))
					);
				}
			} else {
				destroyedOfNotCreated += 1;
			}
			totalDestroyed += 1;
			trajectoriesByEntity.remove(eventRecord.entityId);

		} else {
			final Index toIndex = indexOf(transitionEvent.getNewState());
			this.getCell(toIndex).increment(eventRecord);
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
	public Index indexOf(EntityState ite) {
		final var partitionParts = new Object[partitionsCatalog.getPartitions().size()];
		for (Partition partition : partitionsCatalog.getPartitions()) {
			partitionParts[partition.getOrdinal()] = partition.discriminator().apply(ite);
		}
		return new Index(partitionParts);
	}

	/**
	 * Gives the {@link Cell} pointed by the specified {@link Index}
	 */
	public Cell getCell(Index index) {
		Cell cell = this.cells.get(index);
		if (cell == null) {
			cell = new Cell();
			this.cells.put(index, cell);
		}
		return cell;
	}

	@Getter
	@RequiredArgsConstructor
	@ToString
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
	public static class Cell {
		private int population;
		private int variation;
		private final Set<String> present = new HashSet<>();
		private int addedWhenAlreadyPresent = 0;
		private int removedWhenAbsent = 0;

		private Cell(int population) {
			this.population = population;
		}

		private void increment(EventRecord eventRecord) {
			this.population += 1;
			this.variation += 1;
			var entityId = eventRecord.getEntityId();

			var wasAbsent = this.present.add(entityId);
			if (!wasAbsent) { addedWhenAlreadyPresent += 1; }
		}

		private void decrement(EventRecord eventRecord) {
			this.population -= 1;
			this.variation -= 1;
			var entityId = eventRecord.getEntityId();
			var wasPresent = this.present.remove(entityId);
			if (!wasPresent) {
				removedWhenAbsent += 1;
			}
		}

		public String toString() {
			return "Cell(population=" + this.population + ", variation=" + this.variation + ", present=" + this.present.size()
					+ ", addedWhenAlreadyPresent=" + this.addedWhenAlreadyPresent + ", removedWhenAbsent=" + this.removedWhenAbsent + ")";
		}
	}

	/**
	 * A multidimensional index. Instances of this class identify a {@link Cell} of the entity's discrete state space.
	 */
	public static class Index {
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

		public String toString() {return Arrays.deepToString(this.indexValues);}
	}

	public boolean checkTrajectory(List<EventRecord> trajectory) {
		for (int i = 1; i < trajectory.size(); ++i) {
			if (!trajectory.get(i).oldStateRawJson.equals(trajectory.get(i - 1).newStateRawJson)) {
				LinkedList<EventRecord> looseLinks = new LinkedList<>(trajectory);
				var first = looseLinks.pollFirst();
				return areSortable(first.newStateRawJson, first.oldStateRawJson, looseLinks);
			}
		}
		return true;
	}


	public boolean areSortable(String newerSide, String olderSide, LinkedList<EventRecord> looseLinks) {
		if (looseLinks.isEmpty()) {
			return true;
		} else {
			for (var link : looseLinks) {
				if (link.oldStateRawJson.equals(newerSide)) {
					looseLinks.removeFirstOccurrence(link);
					return areSortable(link.newStateRawJson, olderSide, looseLinks);
				} else if (link.newStateRawJson.equals(olderSide)) {
					looseLinks.removeFirstOccurrence(link);
					return areSortable(newerSide, link.oldStateRawJson, looseLinks);
				}
			}
			return false;
		}
	}


}
