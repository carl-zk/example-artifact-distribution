package com.example.agent.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * @author carl
 * @date 6/12/26 7:13 AM
 */
public class ResumeStore {
	final Path resumeDir = Path.of("./resume");

	public ResumeStore() throws IOException {
		Files.createDirectories(resumeDir);
	}

	public long loadOffset(String fileId) throws IOException {
		Path file = resumeDir.resolve(fileId + ".offset");
		if (!Files.exists(file)) {
			return 0;
		}
		return Long.parseLong(Files.readString(file));
	}

	public void saveOffset(String fileId, long offset) throws IOException {
		Path file = resumeDir.resolve(fileId + ".offset");
		Files.writeString(file, String.valueOf(offset));
	}

	public void complete(String fileId) throws IOException {
		Files.deleteIfExists(resumeDir.resolve(fileId + ".offset"));
	}
}