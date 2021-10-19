package com.mercadolibre.flowbacklogchecker.consolidation;

import java.util.function.Function;

/**
 * A criteria that divides the discrete state space of an entity in parts, where "discrete state space" is the set of
 * all possible particular conditions that said entity can be.
 */
public interface Partition {
	/**
	 * The name of the column of the "backlog_photo_cell" table that indexes this partition. Each possible value of said
	 * column is an index to a part of this partition.
	 */
	String getColumnName();

	/**
	 * The ordinal number of this partition in the partitions well-ordered set.
	 */
	int getOrdinal();

	/**
	 * The function that, when applied to the state of an entity, gives the index of the part of this partition that
	 * contains said state.
	 */
	Function<EntityState, Object> discriminator();
}