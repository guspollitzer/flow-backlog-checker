package com.mercadolibre.flowbacklogchecker.consolidation;


import io.netty.util.collection.LongObjectHashMap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Counts how many entities are in each of the discriminated state subsets, for all entities whose
 * state transition events were integrated into this backlog.
 *
 * <p>Note that this is a mutable class that delegates the responsibility of its content
 * initialization to the user.
 *
 * <p>Design note: This class user's code would be much more clear if this class was immutable. The
 * mutable approach was chosen to improve memory usage and avoid the addition of a dependency to an
 * immutable collections library.
 */
@Slf4j
public class Backlog {
	public static final int CELLS_HASH_MAP_INITIAL_CAPACITY = 8192;

	public final PartitionsCatalog partitionsCatalog;

	/** The arrival serial number of the last event that was merged into this backlog. */
	public long lastEventArrivalSerialNumber;

	public Instant lastEventArrivalDate;

	public final Map<Coordinates, CellContent> cells;

	public final LongObjectHashMap<Trajectory> trajectoriesByEntity;

	/** the amount of entities that where created */
	public int created = 0;
	/** the amount of entities that where destroyed */
	public int terminatedSuccessfully = 0;

	public int discardedEvents = 0;

	public int irregularTrajectories = 0;
	/**
	 * Creates an under construction {@link Backlog} designed to be completed, by means of the loadCell method, with all the cells of a photo of a previous backlog.
	 *
	 * @param partitionsCatalog determines the criteria on how the entities' state space is
	 *     partitioned.
	 * @param lastEventArrivalSerialNumber the arrival serial number of the last transition event
	 *     integrated to this backlog state.
	 * @param lastEventArrivalDate the arrival date of the last transition event integrated to this
	 *     backlog state.
	 */
	public Backlog(
			final PartitionsCatalog partitionsCatalog,
			final long lastEventArrivalSerialNumber,
			final Instant lastEventArrivalDate) {
		this.partitionsCatalog = partitionsCatalog;
		this.lastEventArrivalSerialNumber = lastEventArrivalSerialNumber;
		this.lastEventArrivalDate = lastEventArrivalDate;

		this.cells = new HashMap<>(CELLS_HASH_MAP_INITIAL_CAPACITY);
		this.trajectoriesByEntity = new LongObjectHashMap<>(65536);
	}

	/**
	 * @return the arrival date of the last integrated event.
	 */
	public Instant getLastEventArrivalDate() {
		return this.lastEventArrivalDate;
	}

	/**
	 * @return an estimate of the number of non-empty cells
	 */
	public int getNumberOfCellsTraversed() {
		return cells.size();
	}

	/**
	 * @return the arrival serial number of the last integrated event.
	 */
	public long getLastEventArrivalSerialNumber() {
		return this.lastEventArrivalSerialNumber;
	}

	/**
	 * Updates this {@link Backlog} decrementing the population of the cell that contains the old
	 * state ({@link TransitionEvent#getOldState()}), and incrementing the population of the cell that
	 * contains the new state ({@link TransitionEvent#getNewState()}).
	 *
	 * @param transitionEvent the transition event to integrate into this instance.
	 */
	public void integrate(final TransitionEvent transitionEvent) {
		assert (transitionEvent.getArrivalSerialNumber() > this.lastEventArrivalSerialNumber);
		this.lastEventArrivalSerialNumber = transitionEvent.getArrivalSerialNumber();
		this.lastEventArrivalDate = transitionEvent.getArrivalDate();

		var trajectory = trajectoriesByEntity.get(transitionEvent.getEntityId());
		if (trajectory != null || transitionEvent.getOldState() == null) {
			if (trajectory == null) {
				trajectory = new Trajectory();
				trajectoriesByEntity.put(transitionEvent.getEntityId(), trajectory);
			}
			trajectory.events.add(transitionEvent);

			final EntityState oldState = transitionEvent.getOldState();
			if (oldState != null) {
				final Coordinates fromIndex = indexOf(oldState);
				if (this.getCellContent(fromIndex).decrement(1)) {
					this.cells.remove(fromIndex);
				}
			} else {
				created += 1;
			}

			final EntityState newState = transitionEvent.getNewState();
			if (newState != null) {
				final Coordinates toIndex = indexOf(newState);
				this.getCellContent(toIndex).increment(1);

				if (newState.isUltimate()) {
					trajectory.isCompleted = true;
					if (trajectory.getLastState(Object::equals) instanceof BrokenTrajectoryInfo) {
						irregularTrajectories += 1;
						log.warn("Irregular trajectory #{}: {}", irregularTrajectories, trajectory);
					} else {
						terminatedSuccessfully += 1;
						trajectoriesByEntity.remove(transitionEvent.getEntityId());
					}
				}
			}
		} else {
			discardedEvents += 1;
		}
	}

	/**
	 * @return all the {@link Cell}s (that conform the discrete space of the entities state) with
	 *     which this instance was initialized; plus all the {@link Cell}s that, since then, were
	 *     traversed by an entity whose state transitions have been merged to this instance.
	 */
	public Stream<Cell> getCells() {
		return this.cells.entrySet().stream()
				.map(
						e ->
								new Cell(
										e.getKey().indexValues,
										e.getValue().population,
										e.getValue().variation,
										e.getValue().accumulatedPopulation));
	}

	/**
	 * Finds out the {@link Coordinates} of the {@link Cell} corresponding to the specified {@link
	 * EntityState}.
	 */
	public Coordinates indexOf(EntityState ite) {
		final var partitionParts = new Object[partitionsCatalog.getPartitions().size()];
		for (Partition partition : partitionsCatalog.getPartitions()) {
			partitionParts[partition.getOrdinal()] = partition.discriminator().apply(ite);
		}
		return new Coordinates(partitionParts);
	}

	/** Gives the {@link CellContent} pointed by the specified {@link Coordinates} */
	public CellContent getCellContent(Coordinates index) {
		CellContent cell = this.cells.get(index);
		if (cell == null) {
			cell = new CellContent();
			this.cells.put(index, cell);
		}
		return cell;
	}

	/**
	 * The cells are the little parts of the entity state space in which the whole entity's state
	 * space was divided by the known {@link Partition}s. Each cell knows how many entities have their
	 * state inside it.
	 */
	@Getter
	@RequiredArgsConstructor
	public static class Cell {
		final Object[] coordinates;

		final int population;

		final int variation;

		final int accumulatedPopulation;

	}

	/** A mutable register of the content of a {@link Cell} */
	@NoArgsConstructor
	public static class CellContent {
		public int population;

		public int variation;

		public int accumulatedPopulation;

		public CellContent(int population, int accumulatedPopulation) {
			this.population = population;
			this.accumulatedPopulation = accumulatedPopulation;
		}

		public void increment(int quantity) {
			this.population += quantity;
			this.variation += quantity;
			this.accumulatedPopulation += quantity;
		}

		public boolean decrement(int quantity) {
			this.population -= quantity;
			this.variation -= quantity;
			return this.population == 0;
		}

	}

	/**
	 * The coordinates of a cell. Instances of this class identify a {@link Cell} of the entity's
	 * discrete state space.
	 */
	public static class Coordinates {
		public final Object[] indexValues;

		public final transient int hash;

		public Coordinates(Object... indexValues) {
			this.indexValues = indexValues;
			this.hash = Arrays.hashCode(indexValues);
		}

		@Override
		public boolean equals(final Object o) {
			return o instanceof Coordinates
					&& Arrays.equals(this.indexValues, ((Coordinates) o).indexValues);
		}

		@Override
		public int hashCode() {
			return hash;
		}

		@Override
		public String toString() {return Arrays.deepToString(this.indexValues);}
	}


	public interface LastState {}

	@RequiredArgsConstructor
	public static class LastStateSuccess implements  LastState {
		final EntityState value;
	}

	@RequiredArgsConstructor
	public static class BrokenTrajectoryInfo implements LastState {
		final Trajectory trajectory;
		final EntityState newerSide;
		final EntityState olderSide;
		final TransitionEvent lastTransition;
		final List<TransitionEvent> looseLinks;
	}

	public static class Trajectory {
		List<TransitionEvent> events = new ArrayList<>(16);
		boolean isCompleted = false;

		public interface Comparator {
			boolean areEqual(EntityState a, EntityState b);
		}

		public LastState getLastState(Comparator comparator) {
			for (int i = 1; i < events.size(); ++i) {
				if (!comparator.areEqual(events.get(i).getOldState(), events.get(i - 1).getNewState())) {
					LinkedList<TransitionEvent> looseLinks = new LinkedList<>(events);
					var first = looseLinks.pollFirst();
					assert first != null;
					return getLastStateOf_loop(comparator, first.getNewState(), first.getOldState(), looseLinks);
				}
			}
			return new LastStateSuccess(events.get(events.size() - 1).getNewState());
		}

		public LastState getLastStateOf_loop(final Comparator comparator, final EntityState newerSide, final EntityState olderSide, LinkedList<TransitionEvent> looseLinks) {
			if (looseLinks.isEmpty()) {
				return new LastStateSuccess(olderSide);
			} else {
				var lastTransition = looseLinks.getLast();
				for (var link : looseLinks) {
					if (comparator.areEqual(link.getOldState(), newerSide)) {
						looseLinks.removeFirstOccurrence(link);
						return getLastStateOf_loop(comparator, link.getNewState(), olderSide, looseLinks);
					} else if (comparator.areEqual(link.getNewState(), olderSide)) {
						looseLinks.removeFirstOccurrence(link);
						return getLastStateOf_loop(comparator, newerSide, link.getOldState(), looseLinks);
					}
				}
				return new BrokenTrajectoryInfo(this, newerSide, olderSide, lastTransition, looseLinks);
			}
		}

		@Override
		public String toString() {
			var x = events.stream().map(TransitionEvent::toString).collect(Collectors.joining("\n\t", "[\n\t", "]"));
			return String.format("{isComplete:%b, events:%s", isCompleted, x);
		}
	}
}
