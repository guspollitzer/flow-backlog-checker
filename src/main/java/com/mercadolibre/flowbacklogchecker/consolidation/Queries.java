package com.mercadolibre.flowbacklogchecker.consolidation;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

public class Queries {

	public final Backlog backlog;
	public final Timestamp now = Timestamp.from(Instant.now());

	public final Map<Backlog.Coordinates, Backlog.CellContent> cells;
	public final Map<String, List<TransitionEvent>> trajectoriesByEntity;

	public Queries(Backlog backlog) {
		this.backlog = backlog;
		this.cells = backlog.cells;
		this.trajectoriesByEntity = backlog.trajectoriesByEntity;
	}

	public <T> T eval(final Function<Queries, T> f) {
		return f.apply(this);
	}


	@NoArgsConstructor
	public static class IntAccum {
		int register = 0;

		public String toString() {return Integer.toString(register);}
	}

	@EqualsAndHashCode
	public static class Key implements Comparable<Key> {
		public final Object[] key;
		private transient String keyBis;

		public Key(Object[] key) {
			this.key = key;
			this.keyBis = Arrays.deepToString(key);
		}

		public String toString() {return keyBis;}

		@Override
		public int compareTo(Key o) {
			return keyBis.compareTo(o.keyBis);
		}
	}

	public Map<Key, IntAccum> cellBasedPopulationGrouped(Predicate<Object[]> coordinatesFilter, Predicate<Backlog.CellContent> contentFilter, int... coordinatesToGroupBy) {
		return cells.entrySet().stream()
				.filter(c -> coordinatesFilter.test(c.getKey().indexValues) && contentFilter.test(c.getValue()))
				.reduce(
						new TreeMap<Key, IntAccum>(),
						(acc, c) -> {
							var key = new Key(Arrays.stream(coordinatesToGroupBy).mapToObj(pi -> c.getKey().indexValues[pi]).toArray());
							var intAccum = acc.get(key);
							if (intAccum == null) {
								intAccum = new IntAccum();
								acc.put(key, intAccum);
							}
							intAccum.register += c.getValue().population;
							return acc;
						},
						(a, b) -> {a.putAll(b); return a;}
				);
	}

	public Map<Key, IntAccum> trajectoryBasedPopulationGrouped(Predicate<List<TransitionEvent>> trajectoryFilter, int... coordinatesToGroupBy) {
		return trajectoriesByEntity.values().stream()
				.filter(trajectoryFilter)
				.reduce(
						new TreeMap<Key, IntAccum>(),
						(acc, trajectory) -> {
							var state = Backlog.getLastStateOf(trajectory);
							var key = new Key(Arrays.stream(coordinatesToGroupBy).mapToObj(pi -> PartitionsCatalog.PartitionsDb.values()[pi].valueGetter.apply(state)).toArray());
							var intAccum = acc.get(key);
							if (intAccum == null) {
								intAccum = new IntAccum();
								acc.put(key, intAccum);
							}
							intAccum.register += 1;
							return acc;
						},
						(a, b) -> {
							a.putAll(b);
							return a;
						}
				);
	}

	public Map<Key, List<List<TransitionEvent>>> trajectoryBasedGrouping(
			Predicate<List<TransitionEvent>> trajectoryFilter,
			int... coordinatesToGroupBy
	) {
		return trajectoriesByEntity.entrySet().stream()
				.filter(e -> trajectoryFilter.test(e.getValue()))
				.reduce(
						new TreeMap<Key, List<List<TransitionEvent>>>(),
						(acc, e) -> {
							final var trajectory = e.getValue();
							final var lastState = Backlog.getLastStateOf(trajectory);
							return addTrajectoryToGroup(
									acc,
									trajectory,
									lastState,
									coordinatesToGroupBy
							);
						},
						(a, b) -> {
							a.putAll(b);
							return a;
						}
				);
	}

	public Map<Key, List<List<TransitionEvent>>> brokenTrajectories(
			Predicate<Backlog.UncertainEntityState> stateFilter,
			boolean groupByNewerOrOlderSide,
			int... sideCoordinatesToGroupBy
	) {
		return trajectoriesByEntity.entrySet().stream()
				.filter(e -> {
					var uncertainState = Backlog.getLastStateOf(e.getValue());
					if (uncertainState instanceof Backlog.UncertainEntityState) {
						return stateFilter.test((Backlog.UncertainEntityState)uncertainState);
					} else {
						return false;
					}
				})
				.reduce(
						new TreeMap<Key, List<List<TransitionEvent>>>(),
						(acc, e) -> {
							final var trajectory = e.getValue();
							final var lastState = (Backlog.UncertainEntityState)Backlog.getLastStateOf(trajectory);
							final var side = groupByNewerOrOlderSide && lastState.getNewerSide() != null ? lastState.getNewerSide() : lastState.getOlderSide();
							return addTrajectoryToGroup(acc, trajectory, side, sideCoordinatesToGroupBy);
						},
						(a, b) -> {
							a.putAll(b);
							return a;
						}
				);
	}

	private TreeMap<Key, List<List<TransitionEvent>>> addTrajectoryToGroup(
			TreeMap<Key, List<List<TransitionEvent>>> groupedTrajectories,
			List<TransitionEvent> trajectory,
			EntityState state,
			int[] stateCoordinatesToGroupBy
	) {
		var key = new Key(
				Arrays.stream(stateCoordinatesToGroupBy)
						.mapToObj(pi -> PartitionsCatalog.PartitionsDb.values()[pi].valueGetter.apply(state))
						.toArray()
		);
		var group = groupedTrajectories.computeIfAbsent(key, k -> new ArrayList<>());
		group.add(trajectory);
		return groupedTrajectories;
	}
}
