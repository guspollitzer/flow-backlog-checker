package com.mercadolibre.flowbacklogchecker.consolidation;

/**
 * Specifies the interface of a source of events.
 */
@FunctionalInterface
public interface EventsSource {

	@FunctionalInterface
	interface Sink {
		void accept(EventRecord eventRecord);
	}

	@FunctionalInterface
	interface ContextPredicate {
		boolean test();
	}

	/**
	 * Push events to the specified {@link Sink} while the specified {@link ContextPredicate} evaluates to true. The
	 * events are pushed in ascending sequence number order, starting from the specified sequence number exclusive.
	 *
	 * @param startingEventSerialNumberExclusive specifies which event will be the first one to be sent to the sink: the
	 *        one with the lowest {@link EventRecord#arrivalSerialNumber} that is greater than this parameter.
	 * @param whileCondition events are provided while this predicate evaluates to `true`.
	 * @param sink the consumer of the provided events.
	 */
	void provideWhile(
			long startingEventSerialNumberExclusive,
			ContextPredicate whileCondition,
			Sink sink
	);
}
