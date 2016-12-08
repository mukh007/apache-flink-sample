package org.mj.flink.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.mj.flink.source.SimpleStringGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Class to consume data
 */
public class SimpleSink<T> implements SinkFunction<T> {
	private final static Logger LOG = LoggerFactory.getLogger(SimpleStringGenerator.class);
	private static final long serialVersionUID = 119007289730474249L;
	boolean running = true;

	@Override
	public void invoke(T next) throws Exception {
		LOG.debug("Consumed {}", next);
	}

}