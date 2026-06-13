package com.example.agent.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * represents one active downlaod
 *
 * @author carl
 * @date 6/12/26 7:06 AM
 */
public class DownloadSession implements AutoCloseable {
	final FileChannel channel;

	public DownloadSession(Path file) throws IOException {
		this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
	}

	public synchronized void write(long offset, byte[] bytes) throws IOException {
		channel.write(ByteBuffer.wrap(bytes), offset);
	}

	@Override
	public void close() throws Exception {
		channel.force(true);
		channel.close();
	}
}