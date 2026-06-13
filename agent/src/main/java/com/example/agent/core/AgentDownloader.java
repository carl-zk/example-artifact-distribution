package com.example.agent.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.UUID;

import com.example.agent.util.HashUtil;
import com.example.grpc.proto.artifact.distribution.CompletionRequest;
import com.example.grpc.proto.artifact.distribution.CompletionResponse;
import com.example.grpc.proto.artifact.distribution.FileChunk;
import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author carl
 * @date 6/12/26 7:24 AM
 */
public class AgentDownloader {
	public static final Logger LOGGER = LoggerFactory.getLogger(AgentDownloader.class);

	final ResumeStore resumeStore;

	final FileTransferServiceGrpc.FileTransferServiceBlockingStub stub;

	public AgentDownloader(String host, int port) throws IOException {
		resumeStore = new ResumeStore();
		ManagedChannel channel = ManagedChannelBuilder
				.forAddress(host, port)
				.maxInboundMessageSize(32 * 1024 * 1024)
				.usePlaintext().build();
		stub = FileTransferServiceGrpc.newBlockingStub(channel);
	}

	public void download(String fileId) {
		download(fileId, true);
	}

	private void download(String fileId, boolean failover) {
		try {
			LOGGER.info("downloading file {}", fileId);
			long offset = resumeStore.loadOffset(fileId);

			Iterator<FileChunk> chunks = createStream(fileId, offset);

			Path target = Path.of("/tmp/data", fileId);

			try (DownloadSession session = new DownloadSession(target)) {
				while (true) {
					try {
						if (!chunks.hasNext()) {
							break;
						}
						FileChunk chunk = chunks.next();
						if (chunk.getLastChunk()) {
							break;
						}
						offset = processChunk(fileId, chunk, session);
					}
					catch (ChunckVerificationException ex) {
						chunks = retryChunk(fileId, offset, session);
						offset = resumeStore.loadOffset(fileId);
					}
				}
				complete(fileId, target);
			}
			catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
				if (failover) {
					// delete .offset and try again
					resumeStore.complete(fileId);
					download(fileId, false);
				}
			}
		}
		catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private Iterator<FileChunk> createStream(String fileId, long offset) {
		FileRequest request = FileRequest.newBuilder()
				.setTransferId(UUID.randomUUID().toString())
				.setFileId(fileId)
				.setStartOffset(offset).build();
		return stub.downloadFile(request);
	}

	private long processChunk(String fileId, FileChunk chunk, DownloadSession session) throws IOException {
		LOGGER.info("processing chunk={} fileId={}", chunk.getOffset(), fileId);
		byte[] bytes = chunk.getData().toByteArray();
		verifyChunck(chunk, bytes);
		session.write(chunk.getOffset(), bytes);
		long nextOffset = chunk.getOffset() + bytes.length;
		resumeStore.saveOffset(fileId, nextOffset);
		return nextOffset;
	}

	private void verifyChunck(FileChunk chunk, byte[] bytes) {
		String actual = HashUtil.sha256(bytes);
		if (!actual.equals(chunk.getSha256())) {
			throw new ChunckVerificationException(null, chunk.getOffset());
		}
	}

	private Iterator<FileChunk> retryChunk(String fileId, long failedOffset, DownloadSession session) throws IOException {
		int retries = 0;
		while (true) {
			retries++;
			Iterator<FileChunk> replacement = reconnect(fileId, failedOffset, retries);
			if (!replacement.hasNext()) {
				throw new IOException("Server retruned no chunk! fileId: " + fileId + ", offset: " + failedOffset);
			}
			FileChunk chunk = replacement.next();
			try {
				byte[] bytes = chunk.getData().toByteArray();
				verifyChunck(chunk, bytes);
				session.write(chunk.getOffset(), bytes);
				long nextOffset = chunk.getOffset() + bytes.length;
				resumeStore.saveOffset(fileId, nextOffset);
				LOGGER.info("resume chunk {} at offset {}", fileId, nextOffset);
				return replacement;
			}
			catch (ChunckVerificationException ex) {
				if (retries >= 3) {
					throw ex;
				}
			}
		}
	}

	private Iterator<FileChunk> reconnect(String fileId, long failedOffset, int attempt) {
		LOGGER.warn("Chunk verification failed. reconnecting from offset={}, attempt={}, fileId {}", failedOffset, attempt, fileId);
		return createStream(fileId, failedOffset);
	}

	private void complete(String fileId, Path target) throws IOException {
		LOGGER.info("Verifying completed fileId={}", fileId);
		String finalHash = HashUtil.fileSha256(target);
		LOGGER.info("Finalizing download fileId={}, size={} bytes, hash={}", fileId, Files.size(target), finalHash);
		CompletionResponse response = stub.reportCompletion(CompletionRequest.newBuilder()
				.setFileId(fileId)
				.setFinalHash(finalHash).build());
		if (!response.getSuccess()) {
			throw new IOException("Server rejected completion for fileId=" + fileId + ", reason=" + response.getMessage());
		}
		LOGGER.info("File download completed successfully. fileId={}, hash={}", fileId, finalHash);
		resumeStore.complete(fileId);
	}
}