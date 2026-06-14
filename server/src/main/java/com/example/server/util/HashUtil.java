package com.example.server.util;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import com.dynatrace.hash4j.hashing.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author carl
 * @date 6/11/26 10:11 PM
 */
public final class HashUtil {
	public static final Logger LOGGER = LoggerFactory.getLogger(HashUtil.class);

	static final HexFormat HEX = HexFormat.of();

	private HashUtil() {
	}

	public static long xxh3(byte[] bytes) {
		return Hashing.xxh3_64().hashBytesToLong(bytes);
	}

	public static String sha256(byte[] data) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			return HEX.formatHex(md.digest(data));
		}
		catch (java.security.NoSuchAlgorithmException e) {
			LOGGER.error("SHA-256 algorithm not found", e);
			throw new RuntimeException(e);
		}
	}

	public static String fileSha256(Path file) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");

			byte[] buffer = new byte[4 * 1024 * 1024];

			try (InputStream in = Files.newInputStream(file)) {
				int read;
				while ((read = in.read(buffer)) > 0) {
					digest.update(buffer, 0, read);
				}
			}
			return HEX.formatHex(digest.digest());
		}
		catch (Exception e) {
			LOGGER.error("Error hash256 for file {}", file, e);
			throw new RuntimeException(e);
		}

	}
}