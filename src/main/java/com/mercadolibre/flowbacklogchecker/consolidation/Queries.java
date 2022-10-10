package com.mercadolibre.flowbacklogchecker.consolidation;

import com.mercadolibre.flowbacklogchecker.consolidation.Backlog.BrokenTrajectoryInfo;
import com.mercadolibre.flowbacklogchecker.consolidation.Backlog.LastStateSuccess;
import com.mercadolibre.flowbacklogchecker.consolidation.Backlog.Trajectory;
import io.netty.util.collection.LongObjectHashMap;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.data.util.Pair;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Queries {

	public final Backlog backlog;
	public final Timestamp now = Timestamp.from(Instant.now());

	public final Map<Backlog.Coordinates, Backlog.CellContent> cells;
	public final LongObjectHashMap<Trajectory> trajectoriesByEntity;

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

	public Map<Key, List<Trajectory>> healthyTrajectoryGrouping(
			Predicate<Trajectory> trajectoryFilter,
			Trajectory.Comparator entityStateComparator,
			int... coordinatesToGroupBy
	) {
		return trajectoriesByEntity.entrySet().stream()
				.filter(e -> trajectoryFilter.test(e.getValue()))
				.reduce(
						new TreeMap<Key, List<Trajectory>>(),
						(acc, e) -> {
							final var trajectory = e.getValue();
							final var lastState = trajectory.getLastState(entityStateComparator);
							if (lastState instanceof LastStateSuccess) {
								return addTrajectoryToGroup(
										acc,
										trajectory,
										((LastStateSuccess) lastState).value,
										coordinatesToGroupBy
								);
							} else {
								return acc;
							}
						},
						(a, b) -> {
							a.putAll(b);
							return a;
						}
				);
	}

	public Map<Key, List<Trajectory>> brokenTrajectoriesGrouping(
			Predicate<BrokenTrajectoryInfo> brokenTrajectoryFilter,
			Trajectory.Comparator entityStateComparator,
			boolean groupByNewerOrOlderSide,
			int... sideCoordinatesToGroupBy
	) {
		return trajectoriesByEntity.entrySet().stream()
				.flatMap(e -> {
					var lastState = e.getValue().getLastState(entityStateComparator);
					if (lastState instanceof BrokenTrajectoryInfo) {
						var bti = (BrokenTrajectoryInfo) lastState;
						return brokenTrajectoryFilter.test(bti) ? Stream.of(bti) : Stream.empty();
					} else {
						return Stream.empty();
					}
				})
				.reduce(
						new TreeMap<Key, List<Trajectory>>(),
						(acc, brokenTrajectoryInfo) -> {
							final var side = groupByNewerOrOlderSide && brokenTrajectoryInfo.newerSide != null ? brokenTrajectoryInfo.newerSide : brokenTrajectoryInfo.olderSide;
							return addTrajectoryToGroup(acc, brokenTrajectoryInfo.trajectory, side, sideCoordinatesToGroupBy);
						},
						(a, b) -> {
							a.putAll(b);
							return a;
						}
				);
	}


	public Object genericA(Object... params) {
		return brokenTrajectoriesGrouping(
				bt -> true,
				buildStateComparator(4),
				true,
				0, 1, 3, 5
		);
	}

	public Object genericB(Object... params) {
		return null;
	}

	public Object genericC(Object... params) {
		return null;
	}

	public Trajectory.Comparator buildStateComparator(int... coordinatesToCompare) {
		return (a, b) ->
			a == b
			|| (
					(a != null && b != null) && Arrays.stream(coordinatesToCompare)
					.allMatch(coordinateIndex -> {
						var valueGetter = PartitionsCatalog.PartitionsDb.values()[coordinateIndex].valueGetter;
						return valueGetter.apply(a).equals(valueGetter.apply(b));
					})
			);
	}

	private TreeMap<Key, List<Trajectory>> addTrajectoryToGroup(
			TreeMap<Key, List<Trajectory>> groupedTrajectories,
			Trajectory trajectory,
			EntityState discriminatingState,
			int[] stateCoordinatesToGroupBy
	) {
		var key = new Key(
				Arrays.stream(stateCoordinatesToGroupBy)
						.mapToObj(pi -> PartitionsCatalog.PartitionsDb.values()[pi].valueGetter.apply(discriminatingState))
						.toArray()
		);
		var group = groupedTrajectories.computeIfAbsent(key, k -> new ArrayList<>());
		group.add(trajectory);
		return groupedTrajectories;
	}
}
