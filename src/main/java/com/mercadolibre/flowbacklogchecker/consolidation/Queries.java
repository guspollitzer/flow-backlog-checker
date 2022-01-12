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

	public Queries(Backlog backlog) {
		this.backlog = backlog;
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

	public Map<Key, IntAccum> cellBasedPopulationGrouped(Predicate<Object[]> indexFilter, int... groupedBy) {
		return backlog.getCells()
				.filter(cell -> indexFilter.test(cell.coordinates))
				.reduce(
						new TreeMap<Key, IntAccum>(),
						(acc, cell) -> {
							var key = new Key(Arrays.stream(groupedBy).mapToObj(pi -> cell.coordinates[pi]).toArray());
							var intAccum = acc.get(key);
							if (intAccum == null) {
								intAccum = new IntAccum();
								acc.put(key, intAccum);
							}
							intAccum.register += cell.population;
							return acc;
						},
						(a, b) -> {a.putAll(b); return a;}
				);
	}

	public Map<Key, IntAccum> trajectoryBasedPopulationGrouped(
			Predicate<Backlog.Trajectory> trajectoryFilter,
			int... groupedBy
	) {
		return backlog.trajectories.entrySet().stream()
				.filter(e -> e.getValue() != Backlog.DISCARDED_TRAJECTORY && trajectoryFilter.test(e.getValue()))
				.reduce(
						new TreeMap<Key, IntAccum>(),
						(acc, e) -> {
							var index = Arrays.stream(groupedBy)
									.mapToObj(partitionIndex -> e.getValue().lastCoordinates.indexValues[partitionIndex])
									.toArray();
							var key = new Key(index);
							var value = acc.get(key);
							if (value == null) {
								value = new IntAccum();
								acc.put(key, value);
							}
							value.register += e.getValue().transitions.stream()
									.mapToInt(te -> te.getNewState() != null
											? te.getNewState().getQuantity()
											: te.getOldState() != null
													? -te.getOldState().getQuantity()
													: 0
									).sum();
							return acc;
						},
						(a, b) -> {
							a.putAll(b);
							return a;
						}
				);
	}

	public Map<Key, List<Backlog.Trajectory>> trajectoryBasedGrouping(
			Predicate<Backlog.Trajectory> trajectoryFilter,
			int... groupedBy
	) {
		return backlog.trajectories.entrySet().stream()
				.filter(e -> e.getValue() != Backlog.DISCARDED_TRAJECTORY && trajectoryFilter.test(e.getValue()))
				.reduce(
						new TreeMap<Key, List<Backlog.Trajectory>>(),
						(acc, e) -> {
							var index = Arrays.stream(groupedBy)
									.mapToObj(partitionIndex -> e.getValue().lastCoordinates.indexValues[partitionIndex])
									.toArray();
							var key = new Key(index);
							var group = acc.computeIfAbsent(key, k -> new ArrayList<>());
							group.add(e.getValue());
							return acc;
						},
						(a, b) -> {
							a.putAll(b);
							return a;
						}
				);
	}

}
