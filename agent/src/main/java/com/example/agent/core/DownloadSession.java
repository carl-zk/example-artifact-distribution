package com.example.agent.core;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.example.agent.util.HashUtil;
import com.example.grpc.proto.artifact.distribution.FileChunk;

/**
 * represents one active downlaod
 *
 * @author carl
 * @date 6/12/26 7:06 AM
 */
public class DownloadSession implements AutoCloseable {
	final AsynchronousFileChannel channel;

	final Path file;

	public DownloadSession(Path file) throws Exception {
		this.file = file;
		channel = AsynchronousFileChannel.open(file,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.READ);
	}

	public void write(FileChunk chunk) throws Exception {
		ByteBuffer buffer = chunk.getData().asReadOnlyByteBuffer();
		long position = chunk.getOffset();
		channel.write(buffer, position).get();
	}

	public String finalHash() {
		return HashUtil.fileSha256(file);
	}

	@Override
	public void close() throws Exception {
		channel.close();
	}
}