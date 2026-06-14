package com.example.agent.core;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/12/26 7:24 AM
 */
@Component
@RequiredArgsConstructor
public class AgentDownloader {
	public static final Logger LOGGER = LoggerFactory.getLogger(AgentDownloader.class);

	final ResumeStore resumeStore;

	static final Semaphore SEMAPHORE = new Semaphore(5);

	public CompletableFuture<Void> download(String fileId,
			FileTransferServiceGrpc.FileTransferServiceStub asyncStub,
			FileTransferServiceGrpc.FileTransferServiceBlockingStub completionStub) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			SEMAPHORE.acquire();
			long offset = resumeStore.loadOffset(fileId);
			Path target = Path.of("/tmp/data", fileId);
			DownloadSession session = new DownloadSession(target);
			DownloadObserver observer = new DownloadObserver(
					fileId,
					session,
					resumeStore,
					completionStub,
					future,
					() -> SEMAPHORE.release());
			FileRequest request = FileRequest.newBuilder()
					.setTransferId(UUID.randomUUID().toString())
					.setFileId(fileId)
					.setStartOffset(offset).build();
			LOGGER.info("invode asyncStub.downloadFile");
			asyncStub.downloadFile(request, observer);
			LOGGER.info("invode asyncStub.downloadFile done");
		}
		catch (Exception e) {
			LOGGER.error("invode asyncStub.downloadFile error", e);
			SEMAPHORE.release();
			future.completeExceptionally(e);
		}
		return future;
	}
}