package com.mercadolibre.flowbacklogchecker.consolidation;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Counts how many entities are in each of the discriminated state subsets, for all entities whose state transition
 * events were merged into this backlog.
 */
@Slf4j
public class Backlog {
	private static final int CELLS_HASH_MAP_INITIAL_CAPACITY = 8192;

	public final PartitionsCatalog partitionsCatalog;

	/**
	 * The date of the backlog photo this backlog was initialized with.
	 */
	public final Instant initialPhotoWasTakenOn;

	/**
	 * The arrival serial number of the last event that was merged into this backlog.
	 */
	public long lastEventArrivalSerialNumber;

	public final Map<Coordinates, CellContent> cells;

	public final Map<String, List<TransitionEvent>> trajectoriesByEntity;

	/** the amount of entities that where created */
	public int created = 0;
	/** the amount of entities that where destroyed */
	public int terminatedSuccessfully = 0;

	public int discardedEvents = 0;

	public int irregularTrajectories = 0;

	public Backlog(
			final PartitionsCatalog partitionsCatalog,
			final long lastEventArrivalSerialNumber,
			final Instant initialPhotoWasTakenOn
	) {
		this.partitionsCatalog = partitionsCatalog;
		this.lastEventArrivalSerialNumber = lastEventArrivalSerialNumber;
		this.initialPhotoWasTakenOn = initialPhotoWasTakenOn;

		this.cells = new HashMap<>(CELLS_HASH_MAP_INITIAL_CAPACITY);
		this.trajectoriesByEntity = new HashMap<>(65536);
	}

	public Instant getInitialPhotoWasTakenOn() {
		return this.initialPhotoWasTakenOn;
	}

	/**
	 * Gives the arrival serial number of the last merged event.
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

		var trajectory = trajectoriesByEntity.get(transitionEvent.getEntityId());
		if (trajectory != null || transitionEvent.getOldState() == null) {
			if (trajectory == null) {
				trajectory = new ArrayList<>(16);
				trajectoriesByEntity.put(transitionEvent.getEntityId(), trajectory);
			}
			trajectory.add(transitionEvent);

			final EntityState oldState = transitionEvent.getOldState();
			if (oldState != null) {
				final Coordinates fromIndex = indexOf(oldState);
				if (this.getCellContent(fromIndex).decrement(transitionEvent)) {
					this.cells.remove(fromIndex);
				}
			} else {
				created += 1;
			}

			final EntityState newState = transitionEvent.getNewState();
			if (newState != null && !newState.isUltimate()) {
				final Coordinates toIndex = indexOf(newState);
				this.getCellContent(toIndex).increment(transitionEvent);
			} else {
				if (getLastStateOf(trajectory) instanceof UncertainEntityState) {
					irregularTrajectories += 1;
					log.warn(
							"Irregular trajectory #{}: {}",
							irregularTrajectories,
							trajectory.stream().map(TransitionEvent::toString).collect(Collectors.joining("\n\t", "[\n\t", "]"))
					);
				} else {
					terminatedSuccessfully += 1;
					trajectoriesByEntity.remove(transitionEvent.getEntityId());
				}
			}
		} else {
			discardedEvents += 1;
		}
	}

	/**
	 * Finds out the {@link Coordinates} of the {@link Cell} corresponding to the specified {@link EntityState}.
	 */
	public Coordinates indexOf(EntityState ite) {
		final var partitionParts = new Object[partitionsCatalog.getPartitions().size()];
		for (Partition partition : partitionsCatalog.getPartitions()) {
			partitionParts[partition.getOrdinal()] = partition.discriminator().apply(ite);
		}
		return new Coordinates(partitionParts);
	}

	/**
	 * Gives the {@link CellContent} pointed by the specified {@link Coordinates}
	 */
	public CellContent getCellContent(Coordinates index) {
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
	public static class CellContent {
		public int population;
		public final Set<String> present = new HashSet<>();
		public int addedWhenAlreadyPresent = 0;
		public int removedWhenAbsent = 0;

		public void increment(TransitionEvent transitionEvent) {
			this.population += 1;
			var entityId = transitionEvent.getEntityId();

			var wasAbsent = this.present.add(entityId);
			if (!wasAbsent) { addedWhenAlreadyPresent += 1; }
		}

		public boolean decrement(TransitionEvent transitionEvent) {
			this.population -= 1;
			var entityId = transitionEvent.getEntityId();
			var wasPresent = this.present.remove(entityId);
			if (!wasPresent) {
				removedWhenAbsent += 1;
			}
			return population == 0 && present.isEmpty();
		}

		public String toString() {
			return "Cell(population=" + this.population + ", present=" + this.present.size()
					+ ", addedWhenAlreadyPresent=" + this.addedWhenAlreadyPresent + ", removedWhenAbsent=" + this.removedWhenAbsent + ")";
		}
	}

	/**
	 * The coordinates of a cell. Instances of this class identify a {@link Cell} of the entity's discrete state space.
	 */
	static class Coordinates {
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
		public String toString() {return Arrays.deepToString(this.indexValues);}
	}


	public static EntityState getLastStateOf(List<TransitionEvent> trajectory) {
		for (int i = 1; i < trajectory.size(); ++i) {
			if (!trajectory.get(i).getOldState().equals(trajectory.get(i - 1).getNewState())) {
				LinkedList<TransitionEvent> looseLinks = new LinkedList<>(trajectory);
				var first = looseLinks.pollFirst();
				return getLastStateOf_loop(first.getNewState(), first.getOldState(), looseLinks);
			}
		}
		return trajectory.get(trajectory.size()-1).getNewState();
	}


	public static EntityState getLastStateOf_loop(final EntityState newerSide, final EntityState olderSide, LinkedList<TransitionEvent> looseLinks) {
		if (looseLinks.isEmpty()) {
			return olderSide;
		} else {
			var lastTransition = looseLinks.getLast();
			for (var link : looseLinks) {
				if (link.getOldState().equals(newerSide)) {
					looseLinks.removeFirstOccurrence(link);
					return getLastStateOf_loop(link.getNewState(), olderSide, looseLinks);
				} else if (link.getNewState().equals(olderSide)) {
					looseLinks.removeFirstOccurrence(link);
					return getLastStateOf_loop(newerSide, link.getOldState(), looseLinks);
				}
			}
			return new UncertainEntityState() {
				@Override
				public EntityState getNewerSide() {
					return newerSide;
				}

				@Override
				public EntityState getOlderSide() {
					return olderSide;
				}

				@Override
				public TransitionEvent getLastTransition() {
					return lastTransition;
				}

				public boolean isUltimate() {return this.delegate().isUltimate();}

				public Timestamp getDeadline() {return this.delegate().getDeadline();}

				public String getArea() {return this.delegate().getArea();}

				public String getStatus() {return this.delegate().getStatus();}

				public String getWorkflow() {return this.delegate().getWorkflow();}

				public String getLogisticCenter() {return this.delegate().getLogisticCenter();}

				EntityState delegate() {
					return newerSide != null ? newerSide : olderSide;
				}
			};
		}
	}

	public interface UncertainEntityState extends EntityState {
		EntityState getNewerSide();
		EntityState getOlderSide();
		TransitionEvent getLastTransition();
	}
}
