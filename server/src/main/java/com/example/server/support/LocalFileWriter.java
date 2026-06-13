package com.example.server.support;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;

/**
 *
 * @author carl
 * @date 6/11/26 2:23 PM
 */
public class LocalFileWriter implements ChunkWriter {
	private final Path rootDir;

	public LocalFileWriter(Path rootDir) {
		this.rootDir = rootDir;
	}

	@Override
	public Mono<Void> write(String filename, long offset, Flux<DataBuffer> chunkStream) {
		Path filePath = rootDir.resolve(filename);
		return Mono.fromCallable(() -> {
					if (!Files.exists(filePath)) {
						Files.createDirectories(rootDir);
					}

					return AsynchronousFileChannel.open(
							filePath,
							StandardOpenOption.CREATE,
							StandardOpenOption.WRITE
					);
				})
				.flatMap(channel ->
						DataBufferUtils.write(chunkStream, channel, offset)
								.doFinally(signal -> {
									try {
										channel.close();
									}
									catch (IOException ignored) {
									}
								}).then()
				);
	}
}