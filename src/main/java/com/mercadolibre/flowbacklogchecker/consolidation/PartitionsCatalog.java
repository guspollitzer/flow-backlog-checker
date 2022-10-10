package com.mercadolibre.flowbacklogchecker.consolidation;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * Knows all the {@link Partition} instances.
 */
@Service
public class PartitionsCatalog {

	public final List<Partition> partitions = Arrays.asList(PartitionsDb.values());

	public List<Partition> getPartitions() {
		return partitions;
	}

	public Partition getDeadLinePartition() {
		return PartitionsDb.deadline;
	}

	@RequiredArgsConstructor
	public enum PartitionsDb implements Partition {
		logisticCenter("logistic_center_id", EntityState::getLogisticCenter),
		workflow("workflow", EntityState::getWorkflow),
		area("area", state -> state.getArea() != null ? state.getArea() : "N/A"),
		status("status", EntityState::getStatus),
		dateIn("date_in", EntityState::getDateIn),
		deadline("date_out", EntityState::getDeadline);

		public  final String columnName;

		public  final Function<EntityState, Object> valueGetter;

		@Override
		public String getColumnName() {
			return this.columnName;
		}

		@Override
		public int getOrdinal() {
			return this.ordinal();
		}

		@Override
		public Function<EntityState, Object> discriminator() {
			return this.valueGetter;
		}
	}
}
