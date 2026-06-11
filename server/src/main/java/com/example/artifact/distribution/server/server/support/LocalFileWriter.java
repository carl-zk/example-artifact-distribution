package com.example.artifact.distribution.server.server.support;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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
					Files.createDirectories(rootDir);

					FileChannel channel = FileChannel.open(filePath,
							StandardOpenOption.CREATE,
							StandardOpenOption.WRITE);

					channel.position(offset);
					return channel;
				})
				.flatMap(channel ->
						chunkStream.concatMap(buffer ->
										Mono.fromRunnable(() -> {
													try (DataBuffer.ByteBufferIterator byteBuffer = buffer.readableByteBuffers();) {
														while (byteBuffer.hasNext()) {
															channel.write(byteBuffer.next());
														}
													}
													catch (IOException e) {
														throw new RuntimeException(e);
													}
												}).subscribeOn(Schedulers.boundedElastic())
												.doFinally(signal -> DataBufferUtils.release(buffer))
								).then()
								.doFinally(signal -> {
									try {
										channel.close();
									}
									catch (IOException ignored) {
									}
								})
				);
	}
}