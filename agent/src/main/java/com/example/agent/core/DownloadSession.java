package com.example.agent.core;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;

import com.example.grpc.proto.artifact.distribution.FileChunk;

/**
 * represents one active downlaod
 *
 * @author carl
 * @date 6/12/26 7:06 AM
 */
public class DownloadSession implements AutoCloseable {
	final AsynchronousFileChannel channel;

	final MessageDigest digest;

	public DownloadSession(Path file) throws Exception {
		channel = AsynchronousFileChannel.open(file,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE,
				StandardOpenOption.READ);
		digest = MessageDigest.getInstance("sha-256");
	}

	public void write(FileChunk chunk) throws Exception {
		ByteBuffer buffer = chunk.getData().asReadOnlyByteBuffer();
		ByteBuffer digestBuffer = chunk.getData().asReadOnlyByteBuffer();
		long position = chunk.getOffset();
		channel.write(buffer, position).get();
		digest.update(digestBuffer);
	}

	public String finalHash() {
		return HexFormat.of().formatHex(digest.digest());
	}

	@Override
	public void close() throws Exception {
		channel.close();
	}
}