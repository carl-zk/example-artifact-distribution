package com.example.agent.core;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import com.example.grpc.proto.artifact.distribution.AssignTask;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/14/26 6:36 PM
 */
@Component
public class DownloadTaskRegistry {
	static final Logger LOGGER = LoggerFactory.getLogger(DownloadTaskRegistry.class);

	final Map<Integer, DownloadTaskContext> tasks = new ConcurrentHashMap<>();

	final BlockingDeque<DownloadTaskContext> queue = new LinkedBlockingDeque<>();

	public boolean enqueue(AssignTask assignTask) {
		DownloadTaskContext taskContext = new DownloadTaskContext(assignTask);
		DownloadTaskContext existed = tasks.putIfAbsent(taskContext.assignTask().getTaskId(), taskContext);
		if (existed != null) {
			LOGGER.warn("taskContext already exists, taskId={}, fileName={}", taskContext.assignTask().getTaskId(), taskContext.assignTask().getFileName());
			return false;
		}
		return queue.offer(taskContext);
	}

	public DownloadTaskContext take() throws InterruptedException {
		var c = queue.take();
		tasks.remove(c.assignTask().getTaskId());
		return c;
	}

	@SuppressWarnings("null")
	public Collection<DownloadTaskContext> all() {
		return ImmutableList.copyOf(tasks.values());
	}
}