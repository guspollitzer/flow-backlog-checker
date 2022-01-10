package com.mercadolibre.flowbacklogchecker.consolidation;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Queries {

	public final Backlog backlog;
	public final Map<Backlog.Index, Backlog.Cell> cells;
	public final Map<String, List<EventRecord>> trajectoriesByEntity;

	public Queries(Backlog backlog) {
		this.backlog = backlog;
		this.cells = backlog.cells;
		this.trajectoriesByEntity = backlog.trajectoriesByEntity;
	}

	public long created() {
		return backlog.destroyedOfCreated + backlog.trajectoriesByEntity.entrySet().stream()
				.filter(t -> t.getValue().stream().anyMatch(er -> "null".equals(er.oldStateRawJson))).count(); // 3427719
	}

	public long inProcessPopulation() {
		return cells.entrySet().stream().reduce(0, (a, e) -> a + e.getValue().population, Integer::sum);
	}

	public int outPopulation() {
		return backlog.cells.entrySet().stream()
				.filter(c -> "OUT".equals(c.getKey().indexValues[3]))
				.reduce(0, (a, e) -> a + e.getValue().population, Integer::sum);
	}


	public int pendingPopulation(long fromEpochMilli) {
		return cells.entrySet().stream()
				.filter(c -> "PENDING".equals(c.getKey().indexValues[3]) && ((Timestamp) c.getKey().indexValues[4]).getTime() >= fromEpochMilli)
				.reduce(0, (a, e) -> a + e.getValue().population, Integer::sum); // 200753
	}

	/**
	 * @param fromUtcTimestamp for example: "2021-10-21T03:00:00Z"
	 */
	public Optional<Integer> inPending(String fromUtcTimestamp) {
		return trajectoriesByEntity.values().stream()
				.flatMap(Collection::stream)
				.filter(er -> er.newStateRawJson.indexOf("_departure") > 0 &&
						er.newStateRawJson.substring(er.newStateRawJson.indexOf("_departure") + 13, er.newStateRawJson.indexOf("_departure") + 33)
								.compareTo(fromUtcTimestamp) >= 0)
				.map(er -> (er.oldStateRawJson.contains(":\"PENDING\"") ? -1 : 0) + (er.newStateRawJson.contains(":\"PENDING\"") ? +1 : 0))
				.reduce(Integer::sum); // Optional[215924]
	}

	public int totalAddedWhenAlreadyPresent(long fromEpochMilli) {
		return cells.entrySet().stream()
				.filter(c -> ((Timestamp) c.getKey().indexValues[4]).getTime() > fromEpochMilli)
				.reduce(0, (acc, e) -> acc + e.getValue().addedWhenAlreadyPresent, Integer::sum); // 231430
	}

	public int removedWhenAbsent(long fromEpochMilli) {
		return cells.entrySet().stream()
				.filter(c -> ((Timestamp) c.getKey().indexValues[4]).getTime() > fromEpochMilli)
				.reduce(0, (acc, e) -> acc + e.getValue().removedWhenAbsent, Integer::sum);
	}

	public int removedWhenAbsentAtPending(long fromEpochMilli) {
		return cells.entrySet().stream()
				.filter(c -> "PENDING".equals(c.getKey().indexValues[3]) && ((Timestamp) c.getKey().indexValues[4]).getTime() > fromEpochMilli)
				.reduce(0, (acc, e) -> acc + e.getValue().removedWhenAbsent, Integer::sum);
	}

	public long populationMismatch() {
		return cells.entrySet().stream()
				.filter(c -> c.getValue().population != c.getValue().present.size())
				.count();
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

	public Map<Key, IntAccum> cellBasedPopulationGrouped(Predicate<Backlog.Index> indexFilter, int... groupedBy) {
		return cells.entrySet().stream()
				.filter(c -> indexFilter.test(c.getKey()))
				.reduce(
						new TreeMap<Key, IntAccum>(),
						(acc, c) -> {
							var key = new Key(Arrays.stream(groupedBy).mapToObj(pi -> c.getKey().indexValues[pi]).toArray());
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

	public Map<Key, IntAccum> trajectoryBasedPopulationGrouped(Predicate<String> stateFilter, Function<String, Object>... groupedBy) {
		return trajectoriesByEntity.values().stream()
				.map(t -> {
					var s = Backlog.getLastStateOf(t);
					return s != null ? s : t.get(t.size()-1) + "|irregular";
				})
				.filter(stateFilter)
				.reduce(
						new TreeMap<Key, IntAccum>(),
						(acc, state) -> {
							var key = new Key(Arrays.stream(groupedBy).map(f -> f.apply(state)).toArray());
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

	public List<Map.Entry<String, List<EventRecord>>> trajectoriesWithNoToPendingTransitions() {
		return trajectoriesByEntity.entrySet().stream()
				.filter(x -> x.getValue().stream().allMatch(er -> er.newStateRawJson.contains("ORDER")))
				.filter(x -> x.getValue().stream().allMatch(er -> {
					var i = er.newStateRawJson.indexOf("_departure");
					return er.newStateRawJson.substring(i + 13, i + 33).compareTo("2021-10-21T03:00:00Z") >= 0;
				}))
				.filter(x -> x.getValue().stream().noneMatch(er -> er.newStateRawJson.contains("PENDING")))
				.collect(Collectors.toList());
	}

//	public int populationAtPendingGrouped() {
//		var v = cells.entrySet().stream()
//				.filter(c -> "PENDING".equals(c.getKey().indexValues[3]) && ((Timestamp) c.getKey().indexValues[4]).getTime() > 1634785200000L)
//				.collect(Collector.of(
//						() -> new HashMap<Object[], Integer>(),
//						(Map<Object[], Integer> a, Map.Entry<Backlog.Index, Backlog.Cell> b) -> {
//							a.put(new Object[]{b.getKey().indexValues[0], b.getKey().indexValues[4]}, );
//						},
//						Function.identity(),
//						Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH
//				));
//		return 0;
//	}

	public List<Object[]> pendingPopulationGroupedByHouseAndEtd() {
		return Flux.fromIterable(cells.entrySet())
				.filter(c -> "PENDING".equals(c.getKey().indexValues[3]) && ((Timestamp) c.getKey().indexValues[4]).getTime() > 1634785200000L)
				.groupBy(c -> new Object[]{c.getKey().indexValues[0], c.getKey().indexValues[4]})
				.concatMap(gf -> gf.reduce(0, (acc, c) -> acc + c.getValue().population).map(x -> new Object[]{gf.key(), x}))
				.collectList().block();
	}

	//// ------ Flux based queries --------- ////

	public static class PopEntry implements Comparable<PopEntry> {
		public final Object[] key;
		public final int population;
		private transient String keyBis;

		public PopEntry(Object[] key, int population) {
			this.key = key;
			this.population = population;
			this.keyBis = Arrays.deepToString(key);
		}

		public String toString() {return keyBis + " -> " + population;}

		@Override
		public int compareTo(PopEntry o) {
			return keyBis.compareTo(o.keyBis);
		}
	}

	public List<PopEntry> cellBasedPopulationGrouped_flux(Predicate<Backlog.Index> indexFilter, int... groupedBy) {
		var popEntries = Flux.fromIterable(cells.entrySet())
				.filter(c -> indexFilter.test(c.getKey()))
				.groupBy(c -> Arrays.stream(groupedBy).mapToObj(pi -> c.getKey().indexValues[pi]).toArray())
				.flatMap(gf -> gf
						.reduce(0, (acc, e) -> acc + e.getValue().population)
						.map(pop -> new PopEntry(gf.key(), pop))
				)
				.toIterable();
		return StreamSupport.stream(popEntries.spliterator(), false).sorted().collect(Collectors.toList());
	}


	public List<PopEntry> trajectoryBasedPopulationGrouped_flux(Predicate<String> stateFilter, Function<String, Object>... groupedBy) {
		var popEntries = Flux.fromIterable(trajectoriesByEntity.values())
				.map(t -> {
					var x = Backlog.getLastStateOf(t);
					return x == null ? "irregular" : x;
				})
				.filter(stateFilter)
				.groupBy(state -> Arrays.stream(groupedBy).map(f -> f.apply(state)).toArray())
				.flatMap(gf -> gf
						.count()
						.map(pop -> new PopEntry(gf.key(), pop.intValue()))
				)
				.toIterable();
		return StreamSupport.stream(popEntries.spliterator(), false).sorted().collect(Collectors.toList());
	}

}
