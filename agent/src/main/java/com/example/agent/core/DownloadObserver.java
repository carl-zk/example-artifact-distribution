package com.example.agent.core;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import com.example.agent.util.HashUtil;
import com.example.grpc.proto.artifact.distribution.CompletionRequest;
import com.example.grpc.proto.artifact.distribution.CompletionResponse;
import com.example.grpc.proto.artifact.distribution.FileChunk;
import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import com.example.grpc.proto.artifact.distribution.TransferProcess;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ApplicationEventPublisher;

/**
 *
 * @author carl
 * @date 6/14/26 3:04 PM
 */
public class DownloadObserver implements ClientResponseObserver<FileRequest, FileChunk> {
	public static final Logger LOGGER = LoggerFactory.getLogger(DownloadObserver.class);

	final DownloadTaskContext task;

	final DownloadSession session;

	final ResumeStore resumeStore;

	final FileTransferServiceGrpc.FileTransferServiceBlockingStub completionStub;

	final CompletableFuture<Void> future;

	@Nullable final Runnable completionCallback;

	final ApplicationEventPublisher publisher;

	ClientCallStreamObserver<FileRequest> requestStream;

	public DownloadObserver(DownloadTaskContext task, DownloadSession session, ResumeStore resumeStore,
			FileTransferServiceGrpc.FileTransferServiceBlockingStub completionStub, ApplicationEventPublisher publisher, CompletableFuture<Void> future,
			@Nullable Runnable completionCallback) {
		this.task = task;
		this.session = session;
		this.resumeStore = resumeStore;
		this.completionStub = completionStub;
		this.publisher = publisher;
		this.future = future;
		this.completionCallback = completionCallback;
	}

	@Override
	public void beforeStart(ClientCallStreamObserver<FileRequest> requestStream) {
		this.requestStream = requestStream;
		requestStream.disableAutoRequestWithInitial(1);
		LOGGER.info("beforeStart taskId={}, fileId={}", task.assignTask().getFileId(), task.assignTask().getFileId());
	}

	@Override
	public void onNext(FileChunk chunk) {
		LOGGER.info("onNext taskId={}, fileId={}, chunk offset={}, size={}", task.assignTask().getFileId(), task.assignTask().getFileId(),
				chunk.getOffset(), chunk.getData().size());
		try {
			if (chunk.getLastChunk()) {
				return;
			}
			verifyChunk(chunk);
			session.write(chunk);
			long nextOffset = chunk.getOffset() + chunk.getData().size();
			task.transferred().set(nextOffset);
			resumeStore.saveOffset(task.assignTask().getFileId(), nextOffset);
			requestStream.request(1);
			publisher.publishEvent(TransferProcess.newBuilder()
					.setTaskId(task.assignTask().getTaskId())
					.setTransferred(task.transferred().get())
					.build());
		}
		catch (ChunckVerificationException e) {
			LOGGER.error("chunk verification failed, retry again, taskId={}, fileId={}, offset={}", task.assignTask().getTaskId(), task.assignTask().getFileId(), task.transferred().get());
			future.completeExceptionally(e);
		}
		catch (Exception e) {
			LOGGER.error("download failed taskId={}, fileId={}", task.assignTask().getTaskId(), task.assignTask().getFileId(), e);
			requestStream.cancel("download failed taskId=" + task.assignTask().getTaskId() + ", fileId=" + task.assignTask().getFileId(), e);
			close();
			future.completeExceptionally(e);
		}
	}

	private void verifyChunk(FileChunk chunk) {
		byte[] data = chunk.getData().toByteArray();
		long actualHash = HashUtil.xxh3(data);
		if (actualHash != chunk.getHash()) {
			throw new ChunckVerificationException(null, chunk.getOffset());
		}
	}

	private void close() {
		try {
			session.close();
		}
		catch (Exception e) {
			LOGGER.error("close failed taskId={}, fileId={}", task.assignTask().getTaskId(), task.assignTask().getFileId(), e);
		}
		if (completionCallback != null) {
			completionCallback.run();
		}
	}

	@Override
	public void onError(Throwable t) {
		LOGGER.error("stream error taskId={}, fileId={}", task.assignTask().getTaskId(), task.assignTask().getFileId(), t);
		close();
		future.completeExceptionally(t);
	}

	@Override
	public void onCompleted() {
		try {
			String finalHash = session.finalHash();
			CompletionResponse response = completionStub.reportCompletion(CompletionRequest.newBuilder()
					.setTransferId(task.transferId())
					.setTaskId(task.assignTask().getTaskId())
					.setFileId(task.assignTask().getFileId())
					.setFinalHash(finalHash).build());
			if (!response.getSuccess()) {
				throw new IOException(response.getMessage());
			}
			resumeStore.complete(task.assignTask().getFileId());
			LOGGER.info("download completed taskId={}, fileId={}", task.assignTask().getTaskId(), task.assignTask().getFileId());
			future.complete(null);
		}
		catch (Exception e) {
			future.completeExceptionally(e);
		}
		finally {
			close();
		}
	}
}