package com.example.agent.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/14/26 2:19 PM
 */
@Component
@RequiredArgsConstructor
public class DownloadTaskDispatcher {
	final Semaphore semaphore = new Semaphore(5);

	final ExecutorService taskExecutor = Executors.newVirtualThreadPerTaskExecutor();

	final AgentDownloader downloader;

	void submit(String fileId) {
	}
}