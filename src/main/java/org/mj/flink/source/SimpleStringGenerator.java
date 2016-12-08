package org.mj.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Simple Class to generate data
 */
public class SimpleStringGenerator implements SourceFunction<String> {
	private static final long serialVersionUID = 119007289730474249L;
	boolean running = true;
	long i = 0;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running) {
			ctx.collect("FLINK-" + (i++));
			Thread.sleep(10);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}