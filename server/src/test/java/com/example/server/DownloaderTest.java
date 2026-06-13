package com.example.server;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.function.client.WebClient;

/**
 *
 * @author carl
 * @date 6/11/26 5:37 PM
 */
public class DownloaderTest {
	@Test
	void download_from_uri() throws IOException, URISyntaxException {
		WebClient client = WebClient.create();
		AtomicLong downloaded = new AtomicLong();

		URI uri = new URI("https://releases.ubuntu.com/26.04/ubuntu-26.04-desktop-amd64.iso");
		String fileName = Paths.get(uri.getPath()).getFileName().toString();
		String decoded = URLDecoder.decode(fileName, StandardCharsets.UTF_8);
		Path saveTo = Path.of("/tmp/data/" + decoded);
		// resume download support
		long hasDownloaded = Files.exists(saveTo) ? Files.size(saveTo) : 0;
		downloaded.set(hasDownloaded);

		AsynchronousFileChannel channel =
				AsynchronousFileChannel.open(
						saveTo,
						StandardOpenOption.CREATE,
						StandardOpenOption.WRITE
				);

		client.get()
				.uri(uri)
				.header("Range", "bytes=" + hasDownloaded + "-")
				.retrieve()
				.bodyToFlux(DataBuffer.class)
				.doOnNext(buffer -> {
					long current = downloaded.addAndGet(buffer.readableByteCount());
					System.out.printf(
							"Downloaded: %s (%d bytes)%n",
							humanReadable(current),
							current
					);
				})
				.transform(data -> DataBufferUtils.write(data, channel, hasDownloaded))
				.then()
				.block();
	}

	public static String humanReadable(long bytes) {
		if (bytes < 1024) {
			return bytes + " B";
		}

		String[] units = {"KB", "MB", "GB", "TB", "PB"};
		double value = bytes;
		int unitIndex = -1;

		do {
			value /= 1024;
			unitIndex++;
		} while (value >= 1024 && unitIndex < units.length - 1);

		return String.format("%.2f %s", value, units[unitIndex]);
	}
}