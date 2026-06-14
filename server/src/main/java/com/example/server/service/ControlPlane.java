package com.example.server.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.grpc.proto.artifact.distribution.AgentMessage;
import com.example.grpc.proto.artifact.distribution.AssignTask;
import com.example.grpc.proto.artifact.distribution.ControlPlaneGrpc;
import com.example.grpc.proto.artifact.distribution.RegisterAck;
import com.example.grpc.proto.artifact.distribution.ServerMessage;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author carl
 * @date 6/13/26 5:46 PM
 */
public class ControlPlane extends ControlPlaneGrpc.ControlPlaneImplBase {
	public static final Logger LOGGER = LoggerFactory.getLogger(ControlPlane.class);

	Map<String, StreamObserver<ServerMessage>> agentChannels = new ConcurrentHashMap<>();

	@Override
	public StreamObserver<AgentMessage> connect(StreamObserver<ServerMessage> responseObserver) {
		return new StreamObserver<>() {

			String agentId;

			@Override
			public void onNext(AgentMessage msg) {
				if (msg.hasRegister()) {
					agentId = msg.getRegister().getAgentId();
					agentChannels.put(agentId, responseObserver);
					LOGGER.info("Agent {} connected", agentId);
					responseObserver.onNext(ServerMessage.newBuilder()
							.setRegisterAck(RegisterAck.newBuilder()
									.setAgentId(agentId)
									.setSuccess(true)
									.build())
							.build());
					responseObserver.onNext(ServerMessage.newBuilder()
							.setAssignTask(AssignTask.newBuilder()
									.setTaskId("taskid1")
									.setSource("1")
									.build())
							.build());
				}

				if (msg.hasHeartbeat()) {
					LOGGER.info("Agent {} heartbeat: {}", agentId, msg.getHeartbeat());
				}

				if (msg.hasProgress()) {
					LOGGER.info("Agent {} progress: {} ", agentId, msg.getProgress());
				}
			}

			@Override
			public void onError(Throwable t) {
				agentChannels.remove(agentId);
			}

			@Override
			public void onCompleted() {
				agentChannels.remove(agentId);
			}
		};
	}
}