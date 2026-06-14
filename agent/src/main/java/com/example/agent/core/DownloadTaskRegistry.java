package com.example.agent.core;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/14/26 6:36 PM
 */
@Component
public class DownloadTaskRegistry {
	final Map<String, DownloadTaskContext> tasks = new ConcurrentHashMap<>();

	final BlockingDeque<DownloadTaskContext> queue = new LinkedBlockingDeque<>();

	public boolean enqueue(String taskId, String fileId) {
		DownloadTaskContext ctx = new DownloadTaskContext(taskId, fileId);
		DownloadTaskContext existing = tasks.putIfAbsent(taskId, ctx);
		if (existing != null) {
			return false;
		}
		queue.offer(ctx);
		return true;
	}

	public DownloadTaskContext take() throws InterruptedException {
		return queue.take();
	}

	public DownloadTaskContext get(String taskId) {
		return tasks.get(taskId);
	}

	public Collection<DownloadTaskContext> all() {
		return tasks.values();
	}

	public void markDone(String taskId) {
		tasks.remove(taskId);
	}
}