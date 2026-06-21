package com.example.server.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.example.grpc.proto.artifact.distribution.AgentMessage;
import com.example.grpc.proto.artifact.distribution.AssignTask;
import com.example.grpc.proto.artifact.distribution.ControlPlaneGrpc;
import com.example.grpc.proto.artifact.distribution.RegisterAck;
import com.example.grpc.proto.artifact.distribution.RegisterRequest;
import com.example.grpc.proto.artifact.distribution.ServerMessage;
import com.example.grpc.proto.artifact.distribution.TaskStatus;
import com.example.server.entity.FileRecord;
import com.example.server.entity.Task;
import com.example.server.entity.TaskProgressEvent;
import com.example.server.repository.FileRecordRepository;
import com.example.server.repository.TaskRepository;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/13/26 5:46 PM
 */
@Component
@RequiredArgsConstructor
public class ControlPlane extends ControlPlaneGrpc.ControlPlaneImplBase {
	public static final Logger LOGGER = LoggerFactory.getLogger(ControlPlane.class);

	private final TaskRepository taskRepository;

	private final FileRecordRepository fileRecordRepository;

	Map<String, StreamObserver<ServerMessage>> agentChannels = new ConcurrentHashMap<>();

	CopyOnWriteArrayList<RegisterRequest> agents = new CopyOnWriteArrayList<>();

	final ProgressEventBus eventBus;

	public List<RegisterRequest> listAgentNodes() {
		return agents.stream().toList();
	}

	public Mono<Void> publishTaskOn(List<Integer> taskIds) {
		LOGGER.info("publishTaskOn taskIds={}", taskIds);
		return Flux.fromIterable(taskIds)
				.flatMap(taskRepository::findById)
				.flatMap(task ->
						fileRecordRepository.findById(task.getFileId())
								.map(fileRecord -> Tuples.of(task, fileRecord))
				)
				.doOnNext(t -> send(t.getT1(), t.getT2()))
				.then();
	}

	private void send(Task task, FileRecord fileRecord) {
		StreamObserver<ServerMessage> agentChannel = agentChannels.get(task.getAgentId());
		LOGGER.info("Sending task {} to agent {}", task.getId(), task.getAgentId());
		if (agentChannel != null) {
			agentChannel.onNext(ServerMessage.newBuilder()
					.setAssignTask(AssignTask.newBuilder()
							.setTaskId(task.getId())
							.setFileId(task.getFileId())
							.setFileName(fileRecord.getFileName())
							.setTargetDir(task.getSaveToDir())
							.build()).build());
		}
	}

	@Override
	public StreamObserver<AgentMessage> connect(StreamObserver<ServerMessage> responseObserver) {
		return new StreamObserver<>() {

			String agentId;

			@Override
			public void onNext(AgentMessage msg) {
				if (msg.hasRegister()) {
					agentId = msg.getRegister().getAgentId();
					StreamObserver<ServerMessage> old = agentChannels.put(agentId, responseObserver);
					if (old != null) {
						LOGGER.info("Agent {} has been registered", agentId);
						// agent allowed have multiple active sessions, but I just use the latest one
						agents.removeIf(x -> agentId.equals(x.getAgentId()));
					}
					agents.add(msg.getRegister());
					LOGGER.info("Agent {} connected", agentId);
					responseObserver.onNext(ServerMessage.newBuilder()
							.setRegisterAck(RegisterAck.newBuilder()
									.setAgentId(agentId)
									.setSuccess(true)
									.build())
							.build());
				}

				if (msg.hasHeartbeat()) {
					LOGGER.info("Agent {} heartbeat: {}", agentId, msg.getHeartbeat());
				}

				if (msg.hasProgress()) {
					LOGGER.info("Agent {} progress: {} ", agentId, msg.getProgress());
					eventBus.publish(new TaskProgressEvent(msg.getProgress().getTaskId(), msg.getProgress().getTransferred(), TaskStatus.RUNNING));
				}
			}

			@Override
			public void onError(Throwable t) {
				LOGGER.error("Agent {} error", agentId, t);
				removeAgent(agentId);
			}

			@Override
			public void onCompleted() {
				LOGGER.info("Agent {} completed", agentId);
				removeAgent(agentId);
			}
		};
	}

	void removeAgent(String agentId) {
		agents.removeIf(x -> agentId.equals(x.getAgentId()));
		agentChannels.remove(agentId);
	}
}