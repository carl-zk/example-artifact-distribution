package com.example.server.service;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import com.example.grpc.proto.artifact.distribution.CompletionRequest;
import com.example.grpc.proto.artifact.distribution.CompletionResponse;
import com.example.grpc.proto.artifact.distribution.FileChunk;
import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import com.example.server.entity.FileMetadata;
import com.example.server.util.HashUtil;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author carl
 * @date 6/11/26 10:13 PM
 */
public class FileTransferGrpcService extends FileTransferServiceGrpc.FileTransferServiceImplBase {
	public static final Logger logger = LoggerFactory.getLogger(FileTransferGrpcService.class);

	int chunkSize = 4 * 1024 * 1024; // 4M

	@Override
	public void downloadFile(FileRequest request, StreamObserver<FileChunk> responseObserver) {
		logger.info("downloadFile {}", request.getFileId());
		try {
			String path = "/run/media/carl/E/backUP/哇咔咔/720P_4000K_398011191.mp4";
			long start = System.currentTimeMillis();
			FileMetadata metadata = FileMetadata.builder()
					.fileId("1")
					.path(path)
					.size(1)
					.sha256(HashUtil.fileSha256(Path.of(path)))
					.build();
			long duration = System.currentTimeMillis() - start;
			System.out.println(TimeUnit.MILLISECONDS.toSeconds(duration));
			try (FileChannel channel = FileChannel.open(Path.of(metadata.getPath()), StandardOpenOption.READ)) {
				long offset = request.getStartOffset();
				channel.position(offset);
				ByteBuffer buffer = ByteBuffer.allocateDirect(chunkSize);
				while (channel.read(buffer) > 0) {
					buffer.flip();
					byte[] bytes = new byte[buffer.remaining()];
					buffer.get(bytes);
					responseObserver.onNext(FileChunk.newBuilder()
							.setTransferId(request.getTransferId())
							.setFileId(request.getFileId())
							.setOffset(offset)
							.setSha256(HashUtil.sha256(bytes))
							.setData(ByteString.copyFrom(bytes)).build());
					offset += bytes.length;
					buffer.clear();
				}
				responseObserver.onNext(FileChunk.newBuilder().setLastChunk(true).build());
				responseObserver.onCompleted();
			}
		}
		catch (Exception e) {
			logger.error("request {}", request, e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void reportCompletion(CompletionRequest request, StreamObserver<CompletionResponse> responseObserver) {
		String path = "/run/media/carl/E/backUP/哇咔咔/720P_4000K_398011191.mp4";
		try {
			FileMetadata metadata = FileMetadata.builder()
					.fileId(request.getFileId())
					.path(path)
					.size(Files.size(Path.of(path)))
					.sha256(HashUtil.fileSha256(Path.of(path)))
					.build();
			boolean success = metadata.getSha256().equals(request.getFinalHash());

			responseObserver.onNext(CompletionResponse.newBuilder()
					.setSuccess(success)
					.setMessage(success ? "verified" : "hash mismatch: fileId=%s, agent hash=%s, server hash=%s".formatted(request.getFileId(), request.getFinalHash(), metadata.getSha256())).build());
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			logger.error("request {}", request, e);
			responseObserver.onError(e);
		}

	}
}