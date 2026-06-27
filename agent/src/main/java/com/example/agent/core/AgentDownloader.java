package com.example.agent.core;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.TaskStatus;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEventPublisher;
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

	final DownloadTaskRegistry registry;

	final GrpcClient grpcClient;

	final ApplicationEventPublisher publisher;

	public AgentDownloader(ResumeStore resumeStore, DownloadTaskRegistry registry, GrpcClient grpcClient, ApplicationEventPublisher publisher) {
		this.resumeStore = resumeStore;
		this.registry = registry;
		this.grpcClient = grpcClient;
		this.publisher = publisher;
	}

	@PostConstruct
	protected void start() {
		Thread.ofVirtual().start(this::dispatchLoop);
	}

	private void dispatchLoop() {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				DownloadTaskContext taskContext = registry.take();
				permits.acquire();
				LOGGER.info("dispatch a new task {}", taskContext.assignTask().getTaskId());
				CompletableFuture<Void> future = download(taskContext);
				future.whenComplete((_, throwable) -> {
					permits.release();
					if (throwable == null) {
						taskContext.status().set(TaskStatus.SUCCESS);
					}
					else if (throwable instanceof ChunckVerificationException _) {
						taskContext.status().set(TaskStatus.PENDING);
						if (registry.enqueue(taskContext.assignTask())) {
							LOGGER.info("retry task {}", taskContext.assignTask().getTaskId());
						}
						else {
							taskContext.status().set(TaskStatus.CANCELLED);
							taskContext.message().set("canceled by chunck verification");
							LOGGER.info("retry failed, task {} already exists", taskContext.assignTask().getTaskId());
						}
					}
					else {
						taskContext.status().set(TaskStatus.FAILED);
						taskContext.message().set(throwable.getMessage());
					}
				});
			}
			catch (InterruptedException _) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	public CompletableFuture<Void> download(DownloadTaskContext taskContext) {
		taskContext.status().set(TaskStatus.RUNNING);
		CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			long offset = resumeStore.loadOffset(taskContext.assignTask().getFileId());
			@SuppressWarnings("resource") DownloadSession session = new DownloadSession(Path.of(taskContext.assignTask().getTargetDir(), taskContext.assignTask().getFileName()));
			DownloadObserver observer = new DownloadObserver(taskContext, session, resumeStore, grpcClient.getFileTransferServiceBlockingStub(), publisher, future, null);
			FileRequest request = FileRequest.newBuilder().setTransferId(taskContext.transferId()).setTaskId(taskContext.assignTask().getTaskId()).setFileId(taskContext.assignTask().getFileId()).setStartOffset(offset).build();
			LOGGER.info("invode asyncStub.downloadFile, transferId={}, taskId={}", request.getTransferId(), taskContext.assignTask().getTaskId());
			grpcClient.getFileTransferServiceStub().downloadFile(request, observer);
			LOGGER.info("invode asyncStub.downloadFile done, transferId={}, taskId={}", request.getTransferId(), taskContext.assignTask().getTaskId());
		}
		catch (Exception e) {
			LOGGER.error("download file error", e);
			future.completeExceptionally(e);
		}
		return future;
	}
}