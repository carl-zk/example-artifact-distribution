package com.example.server.service;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.example.grpc.proto.artifact.distribution.CompletionRequest;
import com.example.grpc.proto.artifact.distribution.CompletionResponse;
import com.example.grpc.proto.artifact.distribution.FileChunk;
import com.example.grpc.proto.artifact.distribution.FileRequest;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import com.example.server.entity.FileMetadata;
import com.example.server.util.HashUtil;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author carl
 * @date 6/11/26 10:13 PM
 */
public class FileTransferGrpcService extends FileTransferServiceGrpc.FileTransferServiceImplBase {
	public static final Logger LOGGER = LoggerFactory.getLogger(FileTransferGrpcService.class);

	public static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	int chunkSize = 4 * 1024 * 1024; // 4M

	@Override
	public void downloadFile(FileRequest request, StreamObserver<FileChunk> responseObserver) {
		LOGGER.info("new request downloadFile fileId={}", request.getFileId());

		ServerCallStreamObserver<FileChunk> observer = (ServerCallStreamObserver<FileChunk>) responseObserver;
		try {
			String path = "/run/media/carl/E/backUP/哇咔咔/720P_4000K_398011191.mp4";
			FileMetadata metadata = FileMetadata.builder()
					.fileId("1")
					.path(path)
					.build();
			@SuppressWarnings("resource")
			FileChannel channel = FileChannel.open(Path.of(metadata.getPath()), StandardOpenOption.READ);
			channel.position(request.getStartOffset());
			SendContext ctx = new SendContext(channel, new AtomicLong(
					request.getStartOffset()),
					ByteBuffer.allocate(chunkSize),
					new AtomicBoolean(false),
					new AtomicBoolean(false),
					new AtomicBoolean(false));
			observer.setOnCancelHandler(() -> {
				ctx.completed.set(true);
				close(ctx);
			});
			observer.setOnReadyHandler(() -> executor.submit(() -> {
				LOGGER.info("onReady");
				send(request, observer, ctx);
			}));
			if (observer.isReady()) {
				executor.submit(() -> send(request, observer, ctx));
			}
		}
		catch (Exception e) {
			LOGGER.error("request {}", request, e);
			responseObserver.onError(e);
		}
	}

	private void send(FileRequest request, ServerCallStreamObserver<FileChunk> observer, SendContext ctx) {
		LOGGER.info("send request fileId={}, offset={}, observer is [{}]", request.getFileId(), ctx.offset.get(), observer.isReady());
		try {
			if (!ctx.sending.compareAndSet(false, true)) {
				return;
			}
			while (observer.isReady() && !ctx.closed.get()) {
				ByteBuffer buffer = ctx.buffer;
				int n = ctx.channel.read(buffer);
				if (n < 0) {
					if (ctx.completed.compareAndSet(false, true)) {
						observer.onNext(FileChunk.newBuilder().setLastChunk(true).build());
						observer.onCompleted();
						close(ctx);
					}
					return;
				}
				buffer.flip();
				byte[] bytes = new byte[n];
				buffer.get(bytes);
				observer.onNext(FileChunk.newBuilder()
						.setTransferId(request.getTransferId())
						.setFileId(request.getFileId())
						.setHash(HashUtil.xxh3(bytes))
						.setOffset(ctx.offset.longValue())
						.setData(UnsafeByteOperations.unsafeWrap(bytes)) // less once copy than ByteString.copyFrom(bytes)
						.build());
				ctx.offset.getAndAdd(n);
				buffer.clear();
			}
		}
		catch (Exception ex) {
			LOGGER.error("request {}", request, ex);
			if (ctx.completed.compareAndSet(false, true)) {
				observer.onError(
						Status.INTERNAL
								.withDescription(ex.getMessage())
								.withCause(ex)
								.asRuntimeException());
			}
			close(ctx);
		}
		finally {
			ctx.sending.set(false);
		}
	}

	private void close(SendContext ctx) {
		try {
			if (ctx.closed.compareAndSet(false, true)) {
				ctx.channel.close();
			}
		}
		catch (Exception ignored) {
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
			LOGGER.error("request {}", request, e);
			responseObserver.onError(e);
		}

	}

	record SendContext(FileChannel channel, AtomicLong offset, ByteBuffer buffer,
	                   AtomicBoolean sending,
	                   AtomicBoolean closed,
	                   AtomicBoolean completed) {
	}
}