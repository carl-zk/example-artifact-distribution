package com.example.server.service;

import com.example.server.entity.TaskProgressEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/15/26 9:41 PM
 */
@Component
public class ProgressEventBus {

	final Sinks.Many<TaskProgressEvent> sink = Sinks.many().multicast().directBestEffort();  // never use buffered

	public void publish(TaskProgressEvent event) {
		sink.tryEmitNext(event);
	}

	public Flux<TaskProgressEvent> flux() {
		return sink.asFlux();
	}
}