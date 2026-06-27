package com.example;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.example.agent.AgentApplication;
import com.example.agent.core.AgentDownloader;
import com.example.agent.core.DownloadTaskContext;
import com.example.grpc.proto.artifact.distribution.AssignTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = AgentApplication.class)
public class AgentDownloaderTest {
	Logger logger = LoggerFactory.getLogger(AgentDownloaderTest.class);

	@Autowired(required = true)
	private AgentDownloader downloader;

	@BeforeEach
	public void setUp() {
	}

	@Test
	public void testDownloadSuccess() throws ExecutionException, InterruptedException {
		AssignTask assignTask = com.example.grpc.proto.artifact.distribution.AssignTask.newBuilder()
				.setTaskId(1)
				.setFileId(1)
				.setTargetDir("/tmp/data/agent2")
				.setFileName("RainbowDrops.xml")
				.build();
		DownloadTaskContext context = new DownloadTaskContext(assignTask);

		CompletableFuture<Void> result = downloader.download(context);
		result.whenComplete((v, e) -> {
			logger.info("completed", e);
		});
		result.get();
		assertTrue(result.isDone());
	}
}