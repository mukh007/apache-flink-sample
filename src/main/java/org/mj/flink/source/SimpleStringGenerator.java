package org.mj.flink.source;

import java.util.Random;
//import java.util.UUID;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Class to generate data
 */
public class SimpleStringGenerator implements SourceFunction<String> {
	private static final long serialVersionUID = 119007289730474249L;
	private final static Logger LOG = LoggerFactory.getLogger(SimpleStringGenerator.class);
	boolean running = true;
	private Random random = new Random();
	private int length = 10;

	public SimpleStringGenerator(int length) {
		this.length = length;
	}
	
	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running) {
			String message = "FLINK " + random.nextInt(length);// + " " + UUID.randomUUID();
			LOG.debug("Producing {}", message);
			ctx.collect(message);
			Thread.sleep(10);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}