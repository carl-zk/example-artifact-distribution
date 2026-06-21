package com.example.server.service;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
import com.example.grpc.proto.artifact.distribution.TaskStatus;
import com.example.server.entity.FileRecord;
import com.example.server.entity.TaskProgressEvent;
import com.example.server.repository.FileRecordRepository;
import com.example.server.repository.TaskRepository;
import com.example.server.util.HashUtil;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/11/26 10:13 PM
 */
@AllArgsConstructor
@Component
public class FileTransferGrpcService extends FileTransferServiceGrpc.FileTransferServiceImplBase {
	public static final Logger LOGGER = LoggerFactory.getLogger(FileTransferGrpcService.class);

	public static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	public static final int CHUNK_SIZE = 4 * 1024 * 1024; // 4M

	final FileRecordRepository fileRecordRepository;

	private final TaskRepository taskRepository;

	private final ProgressEventBus eventBus;

	@Override
	public void downloadFile(FileRequest request, StreamObserver<FileChunk> responseObserver) {
		LOGGER.info("new request downloadFile fileId={}", request.getFileId());

		ServerCallStreamObserver<FileChunk> observer = (ServerCallStreamObserver<FileChunk>) responseObserver;
		try {
			FileRecord fileRecord = fileRecordRepository.findById(request.getFileId())
					.blockOptional()
					.orElseThrow(() -> new RuntimeException("fileRecord not  found with id=" + request.getFileId()));
			@SuppressWarnings("resource")
			FileChannel channel = FileChannel.open(Path.of(fileRecord.getFilePath()), StandardOpenOption.READ);
			channel.position(request.getStartOffset());
			SendContext ctx = new SendContext(request, channel, new AtomicLong(
					request.getStartOffset()),
					ByteBuffer.allocate(CHUNK_SIZE),
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
			taskRepository.findById(request.getTaskId())
					.flatMap(task -> {
						task.setStatus(TaskStatus.RUNNING);
						return taskRepository.save(task);
					})
					.doOnError(e -> LOGGER.error("error while saving task", e))
					.subscribe();
		}
		catch (Exception e) {
			LOGGER.error("request {}", request, e);
			observer.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
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
		try {
			LOGGER.info("reportCompletion request {}", request);
			FileRecord fileRecord = fileRecordRepository.findById(request.getFileId()).block();
			assert fileRecord != null;
			boolean success = fileRecord.getSha256().equals(request.getFinalHash());

			responseObserver.onNext(CompletionResponse.newBuilder()
					.setSuccess(success)
					.setMessage(success ? "verified" : "hash mismatch: fileId=%s, agent hash=%s, server hash=%s".formatted(fileRecord.getId(), request.getFinalHash(), fileRecord.getSha256())).build());
			responseObserver.onCompleted();
			if (success) {
				eventBus.publish(new TaskProgressEvent(request.getTaskId(), 0L, TaskStatus.SUCCESS));
				LOGGER.info("eventBus published taskId={} success", request.getTaskId());
				taskRepository.findById(request.getTaskId())
						.flatMap(task -> {
							task.setStatus(TaskStatus.SUCCESS);
							return taskRepository.save(task);
						})
						.doOnError(e -> LOGGER.error("error while saving task", e))
						.subscribe();
			}
		}
		catch (Exception e) {
			LOGGER.error("request {}", request, e);
			responseObserver.onError(e);
		}

	}

	record SendContext(FileRequest fileRequest, FileChannel channel, AtomicLong offset, ByteBuffer buffer,
	                   AtomicBoolean sending,
	                   AtomicBoolean closed,
	                   AtomicBoolean completed) {
	}
}