package com.example.agent.core;

/**
 *
 * @author carl
 * @date 6/13/26 10:31 AM
 */
public class ChunckVerificationException extends RuntimeException {
	private final long failedOffset;

	public ChunckVerificationException(Throwable cause, long failedOffset) {
		super(cause);
		this.failedOffset = failedOffset;
	}

	public long getFailedOffset() {
		return failedOffset;
	}
}