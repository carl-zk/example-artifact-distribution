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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author carl
 * @date 6/14/26 3:04 PM
 */
public class DownloadObserver implements ClientResponseObserver<FileRequest, FileChunk> {
	public static final Logger LOGGER = LoggerFactory.getLogger(DownloadObserver.class);

	final String fileId;

	final DownloadSession session;

	final ResumeStore resumeStore;

	final FileTransferServiceGrpc.FileTransferServiceBlockingStub completionStub;

	final CompletableFuture<Void> future;

	final Runnable completionCallback;

	ClientCallStreamObserver<FileRequest> requestStream;

	public DownloadObserver(String fileId,
			DownloadSession session,
			ResumeStore resumeStore,
			FileTransferServiceGrpc.FileTransferServiceBlockingStub completionStub,
			CompletableFuture<Void> future,
			Runnable completionCallback) {
		this.fileId = fileId;
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
		LOGGER.info("beforeStart fileId={}", fileId);
	}

	@Override
	public void onNext(FileChunk chunk) {
		LOGGER.info("onNext fileId={}, chunk offset={}, size={}", fileId, chunk.getOffset(), chunk.getData().size());
		try {
			if (chunk.getLastChunk()) {
				return;
			}
			verifyChunk(chunk);
			session.write(chunk);
			long nextOffset = chunk.getOffset() + chunk.getData().size();
			resumeStore.saveOffset(fileId, nextOffset);
			requestStream.request(1);
		}
		catch (Exception e) {
			LOGGER.error("download failed fileId={}", fileId, e);
			requestStream.cancel("download failed fileId=" + fileId, e);
			close();
			future.completeExceptionally(e);
		}
	}

	private void verifyChunk(FileChunk chunk) {
		byte[] data = chunk.getData().toByteArray();
		String actualHash = HashUtil.sha256(data);
		if (!actualHash.equals(chunk.getSha256())) {
			throw new ChunckVerificationException(null, chunk.getOffset());
		}
	}

	private void close() {
		try {
			session.close();
		}
		catch (Exception ignored) {}
		completionCallback.run();
	}

	@Override
	public void onError(Throwable t) {
		LOGGER.error("stream error fileId={}", fileId, t);
		close();
		future.completeExceptionally(t);
	}

	@Override
	public void onCompleted() {
		try {
			String finalHash = session.finalHash();
			CompletionResponse response = completionStub.reportCompletion(
					CompletionRequest.newBuilder()
							.setTransferId("")
							.setFileId(fileId)
							.setFinalHash(finalHash).build());
			if (!response.getSuccess()) {
				throw new IOException(response.getMessage());
			}
			resumeStore.complete(fileId);
			LOGGER.info("download completed fileId={}", fileId);
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