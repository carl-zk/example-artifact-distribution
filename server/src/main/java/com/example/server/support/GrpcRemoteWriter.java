package com.example.server.support;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;

/**
 *
 * @author carl
 * @date 6/11/26 2:23 PM
 */
public class GrpcRemoteWriter implements ChunkWriter {
	@Override
	public Mono<Void> write(String filename, long offset, Flux<DataBuffer> chunkStream) {
		return null;
	}
}