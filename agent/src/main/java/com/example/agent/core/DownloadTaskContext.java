package com.example.agent.core;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.example.grpc.proto.artifact.distribution.TaskStatus;

/**
 *
 * @author carl
 * @date 6/14/26 6:32 PM
 */
public record DownloadTaskContext(
		String transferId,
		String taskId,
		String fileId,
		AtomicReference<TaskStatus> status,
		AtomicLong transferred,
		AtomicReference<String> message
) {
	public DownloadTaskContext(String taskId, String fileId) {
		this(UUID.randomUUID().toString(), taskId, fileId, new AtomicReference<>(TaskStatus.PENDING), new AtomicLong(), new AtomicReference<>());
	}
}