package com.mercadolibre.flowbacklogchecker.consolidation;


import java.sql.SQLException;

@FunctionalInterface
public interface EventsSource {

	@FunctionalInterface
	interface Sink {
		/**
		 * The {@link EventsSource} implementation should provide events one by one, synchronously, until this method returns false or the queue
		 * gets empty.
		 *
		 * @return the implementation of the {@link EventsSource#provideWhile(long, Sink)} should continue calling this method while it returns
		 *     true and the events queue is not empty.
		 */
		boolean accept(EventRecord eventRecord);
	}

	/**
	 * The implementation should push queued events to the specified {@link Sink} until it returns false. The
	 * events should be pushed synchronously in ascending arrival serial number order, starting from the specified serial number exclusive.
	 *
	 * @param startingEventArrivalSerialNumberExclusive specifies which event will be the first one to be sent to the sink: the
	 *        one with the lowest {@link EventRecord#getArrivalSerialNumber} that is greater than this parameter.
	 * @param sink the consumer of the provided events and decider of provision continuation.
	 */
	void provideWhile(long startingEventArrivalSerialNumberExclusive, Sink sink) throws SQLException;
}
