package com.mercadolibre.flowbacklogchecker.consolidation;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * Counts how many entities are in each of the discriminated state subsets, for all entities whose state transition
 * events were merged into this backlog.
 */
public class Backlog {
	private static final int CELLS_HASH_MAP_INITIAL_CAPACITY = 8192;

	private final PartitionsCatalog partitionsCatalog;

	/**
	 * The date of the backlog photo this backlog was initialized with.
	 */
	private final Instant initialPhotoWasTakenOn;

	/**
	 * The arrival serial number of the last event that was merged into this backlog.
	 */
	private long lastEventArrivalSerialNumber;

	private final Map<Coordinates, CellContent> cells;


	public Backlog(
			final PartitionsCatalog partitionsCatalog,
			final long lastEventArrivalSerialNumber,
			final Instant initialPhotoWasTakenOn
	) {
		this.partitionsCatalog = partitionsCatalog;
		this.lastEventArrivalSerialNumber = lastEventArrivalSerialNumber;
		this.initialPhotoWasTakenOn = initialPhotoWasTakenOn;

		this.cells = new HashMap<>(CELLS_HASH_MAP_INITIAL_CAPACITY);
	}

	public Instant getInitialPhotoWasTakenOn() {
		return this.initialPhotoWasTakenOn;
	}

	/**
	 * @return an estimate of the number of non-empty cells
	 */
	public int getNumberOfCellsTraversed() {
		return cells.size();
	}

	/**
	 * @return the arrival serial number of the last merged event.
	 */
	public long getLastEventArrivalSerialNumber() {
		return this.lastEventArrivalSerialNumber;
	}

	/**
	 * Updates this {@link Backlog} decrementing the population of the cell that contains the old state ({@link
	 * TransitionEvent#getOldState()}), and incrementing the population of the cell that contains the new state ({@link
	 * TransitionEvent#getNewState()}).
	 *
	 * @param transitionEvent the transition event to merge into this instance.
	 */
	public void merge(final TransitionEvent transitionEvent) {
		assert (transitionEvent.getArrivalSerialNumber() > this.lastEventArrivalSerialNumber);
		this.lastEventArrivalSerialNumber = transitionEvent.getArrivalSerialNumber();

		final EntityState oldState = transitionEvent.getOldState();
		if (oldState != null) {
			final Coordinates fromIndex = indexOf(oldState);
			this.getCellContent(fromIndex).decrement(oldState.getQuantity());
		}
		final EntityState newState = transitionEvent.getNewState();
		if (newState != null && !newState.isUltimate()) {
			final Coordinates toIndex = indexOf(newState);
			this.getCellContent(toIndex).increment(newState.getQuantity());
		}
	}

	/**
	 * Adds a cell to this instance.
	 *
	 * @param coordinates the coordinates o the added {@link Cell}.
	 * @param population how many entities are inside the added {@link Cell}.
	 */
	public void loadCell(Object[] coordinates, int population) {
		this.cells.put(new Coordinates(coordinates), new CellContent(population));
	}

	/**
	 * @return all the {@link Cell}s (that conform the discrete space of the entities state) with which this instance was
	 * initialized; plus all the {@link Cell}s that, since then, were traversed by an entity whose state transitions have
	 * been merged to this instance.
	 */
	public Stream<Cell> getCells() {
		return this.cells.entrySet().stream()
				.map(e -> new Cell(
						e.getKey().indexValues,
						e.getValue().population,
						e.getValue().variation
				));
	}

	/**
	 * Finds out the {@link Coordinates} of the {@link Cell} corresponding to the specified {@link EntityState}.
	 */
	private Coordinates indexOf(EntityState ite) {
		final var partitionParts = new Object[partitionsCatalog.getPartitions().size()];
		for (Partition partition : partitionsCatalog.getPartitions()) {
			partitionParts[partition.getOrdinal()] = partition.discriminator().apply(ite);
		}
		return new Coordinates(partitionParts);
	}

	/**
	 * Gives the {@link CellContent} pointed by the specified {@link Coordinates}
	 */
	private CellContent getCellContent(Coordinates index) {
		CellContent cell = this.cells.get(index);
		if (cell == null) {
			cell = new CellContent();
			this.cells.put(index, cell);
		}
		return cell;
	}

	/**
	 * The cells are the little parts of the entity state space in which the whole entity's state space was divided by the
	 * known {@link Partition}s. Each cell knows how many entities have their state inside it.
	 */
	@Getter
	@RequiredArgsConstructor
	public static class Cell {
		final Object[] coordinates;

		final int population;

		final int variation;
	}

	/**
	 * A mutable register of the content of a {@link Cell}
	 */
	@NoArgsConstructor
	static class CellContent {
		private int population;

		private int variation;

		CellContent(int population) {
			this.population = population;
		}

		private void increment(int quantity) {
			this.population += quantity;
			this.variation += quantity;
		}

		private void decrement(int quantity) {
			this.population -= quantity;
			this.variation -= quantity;
		}
	}

	/**
	 * The coordinates of a cell. Instances of this class identify a {@link Cell} of the entity's discrete state space.
	 */
	static class Coordinates {
		private final Object[] indexValues;

		private final transient int hash;

		Coordinates(Object... indexValues) {
			this.indexValues = indexValues;
			this.hash = Arrays.hashCode(indexValues);
		}

		@Override
		public boolean equals(final Object o) {
			return o instanceof Coordinates && Arrays.equals(this.indexValues, ((Coordinates) o).indexValues);
		}

		@Override
		public int hashCode() {
			return hash;
		}
	}
}
