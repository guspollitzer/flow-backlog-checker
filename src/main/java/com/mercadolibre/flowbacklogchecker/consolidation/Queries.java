package com.mercadolibre.flowbacklogchecker.consolidation;

import java.util.stream.Stream;

public class Queries {

	public final Backlog backlog;
	public final Stream<Backlog.Cell> cells;

	public Queries(Backlog backlog) {
		this.backlog = backlog;
		this.cells = backlog.getCells();
	}

}
