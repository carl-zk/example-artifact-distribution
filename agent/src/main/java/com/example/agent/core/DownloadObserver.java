package com.example.agent.core;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import com.example.agent.util.HashUtil;
import com.example.grpc.proto.artifact.distribution.CompletionRequest;
import com.example.grpc.proto.artifact.distribution.CompletionResponse;
import com.example.grpc.proto.artifact.distribution.FileChunk;
import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	ClientCallStreamObserver<FileRequest> requestStream;

	public DownloadObserver(DownloadTaskContext task, DownloadSession session, ResumeStore resumeStore,
			FileTransferServiceGrpc.FileTransferServiceBlockingStub completionStub, CompletableFuture<Void> future,
			@Nullable Runnable completionCallback) {
		this.task = task;
		this.session = session;
		this.resumeStore = resumeStore;
		this.completionStub = completionStub;
		this.future = future;
		this.completionCallback = completionCallback;
	}

	@Override
	public void beforeStart(ClientCallStreamObserver<FileRequest> requestStream) {
		this.requestStream = requestStream;
		requestStream.disableAutoRequestWithInitial(1);
		LOGGER.info("beforeStart taskId={}, fileId={}", task.taskId(), task.fileId());
	}

	@Override
	public void onNext(FileChunk chunk) {
		LOGGER.info("onNext taskId={}, fileId={}, chunk offset={}, size={}", task.taskId(), task.fileId(),
				chunk.getOffset(), chunk.getData().size());
		try {
			if (chunk.getLastChunk()) {
				return;
			}
			verifyChunk(chunk);
			session.write(chunk);
			long nextOffset = chunk.getOffset() + chunk.getData().size();
			task.transferred().set(nextOffset);
			resumeStore.saveOffset(task.fileId(), nextOffset);
			requestStream.request(1);
		}
		catch (Exception e) {
			LOGGER.error("download failed taskId={}, fileId={}", task.taskId(), task.fileId(), e);
			requestStream.cancel("download failed taskId=" + task.taskId() + ", fileId=" + task.fileId(), e);
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
			LOGGER.error("close failed taskId={}, fileId={}", task.taskId(), task.fileId(), e);
		}
		if (completionCallback != null) {
			completionCallback.run();
		}
	}

	@Override
	public void onError(Throwable t) {
		LOGGER.error("stream error taskId={}, fileId={}", task.taskId(), task.fileId(), t);
		close();
		future.completeExceptionally(t);
	}

	@Override
	public void onCompleted() {
		try {
			String finalHash = session.finalHash();
			CompletionResponse response = completionStub.reportCompletion(CompletionRequest.newBuilder()
					.setTransferId(task.transferId())
					.setFileId(task.fileId())
					.setFinalHash(finalHash).build());
			if (!response.getSuccess()) {
				throw new IOException(response.getMessage());
			}
			resumeStore.complete(task.fileId());
			LOGGER.info("download completed taskId={}, fileId={}", task.taskId(), task.fileId());
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