package com.example.agent.core;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.TaskStatus;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/12/26 7:24 AM
 */
@Component
public class AgentDownloader {
	public static final Logger LOGGER = LoggerFactory.getLogger(AgentDownloader.class);

	final Semaphore permits = new Semaphore(5);

	final ResumeStore resumeStore;

	final Path rootPath;

	final DownloadTaskRegistry registry;

	final GrpcClient grpcClient;

	public AgentDownloader(ResumeStore resumeStore, @Value("${downloader.root-dir}") String rootPath, DownloadTaskRegistry registry, GrpcClient grpcClient) {
		this.resumeStore = resumeStore;
		this.rootPath = Path.of(rootPath);
		this.registry = registry;
		this.grpcClient = grpcClient;
	}

	@PostConstruct
	protected void start() {
		Thread.ofVirtual().start(this::dispatchLoop);
	}

	private void dispatchLoop() {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				DownloadTaskContext task = registry.take();
				permits.acquire();
				LOGGER.info("dispatch a new task {}", task.taskId());
				CompletableFuture<Void> future = download(task);
				future.whenComplete((taskContext, throwable) -> {
					if (throwable == null) {
						task.status().set(TaskStatus.SUCCESS);
						registry.markDone(task.taskId());
					}
					else {
						task.status().set(TaskStatus.FAILED);
						task.message().set(throwable.getMessage());
					}
				});
			}
			catch (InterruptedException _) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	private CompletableFuture<Void> download(DownloadTaskContext task) {
		task.status().set(TaskStatus.RUNNING);
		CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			long offset = resumeStore.loadOffset(task.fileId());
			Path target = Path.of(rootPath.toString(), task.fileId());
			@SuppressWarnings("resource")
			DownloadSession session = new DownloadSession(target);
			DownloadObserver observer = new DownloadObserver(
					task,
					session,
					resumeStore,
					grpcClient.getFileTransferServiceBlockingStub(),
					future,
					permits::release);
			FileRequest request = FileRequest.newBuilder()
					.setTransferId(task.transferId())
					.setFileId(task.fileId())
					.setStartOffset(offset).build();
			LOGGER.info("invode asyncStub.downloadFile, transferId={}, taskId={}", request.getTransferId(), task.taskId());
			grpcClient.getFileTransferServiceStub().downloadFile(request, observer);
			LOGGER.info("invode asyncStub.downloadFile done, transferId={}, taskId={}", request.getTransferId(), task.taskId());
		}
		catch (Exception e) {
			LOGGER.error("invode asyncStub.downloadFile error", e);
			future.completeExceptionally(e);
		}
		return future;
	}
}