package com.example.artifact.distribution.server.server.support;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;

/**
 *
 * @author carl
 * @date 6/11/26 2:21 PM
 */
public interface ChunkWriter {
	Mono<Void> write(String filename, long offset, Flux<DataBuffer> chunkStream);
}