package com.example.agent.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 *
 * @author carl
 * @date 6/12/26 7:19 AM
 */
public final class HashUtil {
	private HashUtil() {
	}

	public static String sha256(byte[] bytes) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			return HexFormat.of().formatHex(digest.digest(bytes));
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	public static String fileSha256(Path file) {
		try (InputStream in = Files.newInputStream(file)) {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");

			byte[] buffer = new byte[4 * 1024 * 1024];
			int read;
			while ((read = in.read(buffer)) > 0) {
				digest.update(buffer, 0, read);
			}

			return HexFormat.of().formatHex(digest.digest());
		}
		catch (NoSuchAlgorithmException | IOException e) {
			throw new RuntimeException(e);
		}
	}
}