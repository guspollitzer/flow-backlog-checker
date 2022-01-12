package com.mercadolibre.flowbacklogchecker.consolidation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * Counts how many entities are in each of the discriminated state subsets, for all entities whose state transition
 * events were merged into this backlog.
 */
public class Backlog {
	public static final int CELLS_HASH_MAP_INITIAL_CAPACITY = 8192;
	public static final Trajectory DISCARDED_TRAJECTORY = new Trajectory(false) {
		@Override
		public void addEvent(TransitionEvent event, Coordinates coordinates) {}
	};

	private final PartitionsCatalog partitionsCatalog;

	/**
	 * The date of the backlog photo this backlog was initialized with.
	 */
	public final Instant initialPhotoWasTakenOn;

	/**
	 * The arrival serial number of the last event that was merged into this backlog.
	 */
	public long lastEventArrivalSerialNumber;

	public final Map<Coordinates, CellContent> cells;
	public final LongObjectMap<Trajectory> trajectories;


	public Backlog(
			final PartitionsCatalog partitionsCatalog,
			final long lastEventArrivalSerialNumber,
			final Instant initialPhotoWasTakenOn
	) {
		this.partitionsCatalog = partitionsCatalog;
		this.lastEventArrivalSerialNumber = lastEventArrivalSerialNumber;
		this.initialPhotoWasTakenOn = initialPhotoWasTakenOn;

		this.cells = new HashMap<>(CELLS_HASH_MAP_INITIAL_CAPACITY);
		this.trajectories = new LongObjectHashMap<>(10000);
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

		Coordinates coordinates = null;
		final EntityState oldState = transitionEvent.getOldState();
		if (oldState != null) {
			coordinates = indexOf(oldState);
			this.getCellContent(coordinates).decrement(oldState.getQuantity());
		}
		final EntityState newState = transitionEvent.getNewState();
		if (newState != null && !newState.isUltimate()) {
			coordinates = indexOf(newState);
			this.getCellContent(coordinates).increment(newState.getQuantity());
		}

		var trajectory = trajectories.get(transitionEvent.getEntityId());
		if (trajectory == null) {
			if (newState != null && "SCHEDULED".equals(newState.getStatus())) {
				trajectory = new Trajectory(true);
			} else if (oldState != null && "SCHEDULED".equals(oldState.getStatus())) {
				trajectory = new Trajectory(false);
			} else {
				trajectory = DISCARDED_TRAJECTORY;
			}
		}
		trajectory.addEvent(transitionEvent, coordinates);
		trajectories.put(transitionEvent.getEntityId(), trajectory);
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

		@Override
		public String toString() {
			return String.format("{%s -> %d(%d)}", Arrays.deepToString(coordinates), population, variation);
		}
	}

	/**
	 * A mutable register of the content of a {@link Cell}
	 */
	@NoArgsConstructor
	public static class CellContent {
		public int population;

		public int variation;

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
	public static class Coordinates {
		public final Object[] indexValues;

		public final transient int hash;

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

		@Override
		public String toString() {
			return Arrays.toString(indexValues);
		}
	}

	public static class Trajectory {
		public final boolean scheduleCreated;
		public final List<TransitionEvent> transitions;
		public Coordinates lastCoordinates;
		public int scheduledQuantity;
		public int checkinQuantity;
		public int putAwayQuantity;
		public int putAwayPsQuantity;
		public int pickQuantity;
		public int otherQuantity;
		public int storedQuantity;
		public boolean completed = false;
		public boolean started = false;

		public Trajectory(boolean scheduleCreated) {
			this.scheduleCreated = scheduleCreated;
			this.transitions = new ArrayList<>(16);
		}

		public void addEvent(TransitionEvent event, Coordinates coordinates) {
			this.lastCoordinates = coordinates;
			this.transitions.add(event);

			final var newState = event.getNewState();
			final var oldState = event.getOldState();
			final String eventType;
			if (newState != null) {
				eventType = newState.getEventType();
				switch(newState.getStatus()) {
					case "SCHEDULED": scheduledQuantity += newState.getQuantity();
						break;
					case "CHECK_IN": checkinQuantity += newState.getQuantity();
						break;
					case "PUT_AWAY": putAwayQuantity += newState.getQuantity();
						break;
					case "PICK": pickQuantity += newState.getQuantity();
						break;
					case "PUTAWAY_PS": putAwayPsQuantity += newState.getQuantity();
						break;
					default: otherQuantity += newState.getQuantity();
				}
			} else if (oldState != null) {
				eventType = oldState.getEventType();
				switch(oldState.getStatus()) {
					case "SCHEDULED": scheduledQuantity -= oldState.getQuantity();
						break;
					case "CHECK_IN": checkinQuantity -= oldState.getQuantity();
						break;
					case "PUT_AWAY":
						putAwayQuantity -= oldState.getQuantity();
						storedQuantity += oldState.getQuantity();
						break;
					case "PICK": pickQuantity -= oldState.getQuantity();
						break;
					case "PUTAWAY_PS":
						putAwayPsQuantity -= oldState.getQuantity();
						storedQuantity += oldState.getQuantity();
						break;
					default: otherQuantity -= oldState.getQuantity();
				}
			} else {
				eventType = "";
			}

			if ("INBOUND_STAGE_INBOUND_FINISHED".equals(eventType)) {
				this.completed = true;
			} else if ("INBOUND_STAGE_INBOUND_CREATED".equals(eventType)) {
				this.started = true;
			}
		}

		@Override
		public String toString() {
			return String.format(
					"(entity: %d, scheduled: %d, checkIn: %d, putAway: %d, pick: %d, putAwayPs: %d, started: %b, completed: %b, coordinates: %s, events: %d)",
					transitions.get(0).getEntityId(), scheduledQuantity, checkinQuantity, putAwayQuantity, pickQuantity, putAwayPsQuantity, started, completed, lastCoordinates, transitions.size()
					);
		}
	}
}
