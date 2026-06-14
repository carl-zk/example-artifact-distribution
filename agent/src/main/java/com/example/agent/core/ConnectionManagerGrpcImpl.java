package com.example.agent.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.grpc.proto.artifact.distribution.AgentMessage;
import com.example.grpc.proto.artifact.distribution.ControlPlaneGrpc;
import com.example.grpc.proto.artifact.distribution.Heartbeat;
import com.example.grpc.proto.artifact.distribution.RegisterRequest;
import com.example.grpc.proto.artifact.distribution.ServerMessage;
import com.example.grpc.proto.artifact.distribution.TaskStatus;
import com.example.grpc.proto.artifact.distribution.TransferProcess;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

import org.springframework.stereotype.Component;

/**
 * 1. 建立控制连接
 * 2. 注册
 * 3. 心跳
 * 4. 接收任务
 * 5. 重连
 *
 * @author carl
 * @date 6/13/26 6:11 PM
 */
@Component
@RequiredArgsConstructor
public class ConnectionManagerGrpcImpl implements ConnectionManager {
	public static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManagerGrpcImpl.class);

	public final String agentId = "abc";

	final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	final ManagedChannel channel;

	volatile StreamObserver<AgentMessage> requestStream;

	final AtomicBoolean connected = new AtomicBoolean(false);

	final AtomicBoolean connecting = new AtomicBoolean(false);

	final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);

	final DownloadTaskRegistry registry;

	final GrpcClient grpcClient;

	@PostConstruct
	@Override
	public void start() {
		scheduler.scheduleAtFixedRate(this::sendHeartbeat,
				30,
				30,
				TimeUnit.SECONDS);
		scheduler.scheduleAtFixedRate(this::sendProgress,
				1,
				1,
				TimeUnit.SECONDS);
		connect();
	}

	private void connect() {
		if (!connecting.compareAndSet(false, true)) {
			return;
		}
		try {
			ControlPlaneGrpc.ControlPlaneStub stub = grpcClient.getControlPlaneStub();
			requestStream = stub.connect(streamObserver);
			sendRegister();
		}
		finally {
			connecting.set(false);
		}
	}

	private StreamObserver<ServerMessage> streamObserver = new StreamObserver<>() {
		@Override
		public void onNext(ServerMessage msg) {
			if (msg.hasAssignTask()) {
				LOGGER.info("new task: {}", msg.getAssignTask().getSource());
				var task = msg.getAssignTask();
				registry.enqueue(task.getTaskId(), task.getSource());
				LOGGER.info("task enqueued taskId={}", task.getTaskId());
			}
			if (msg.hasRegisterAck()) {
				if (msg.getRegisterAck().getSuccess()) {
					connected.set(true);
					LOGGER.info("agent register success");
				}
				else {
					LOGGER.error("agent register failed");
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			LOGGER.error(t.getMessage(), t);
			connected.set(false);
			requestStream = null;
			scheduleReconnect(5);
		}

		@Override
		public void onCompleted() {
			LOGGER.info("completed");
			connected.set(false);
			scheduleReconnect(5);
		}
	};

	private void sendRegister() {
		requestStream.onNext(AgentMessage.newBuilder()
				.setAgentId(agentId)
				.setRegister(RegisterRequest.newBuilder()
						.setAgentId(agentId)
						.setHostname("hosta").build())
				.build());
	}

	@Override
	public synchronized void reconnect() {
	}

	@Override
	public void sendHeartbeat() {
		if (!connected.get() || requestStream == null) {
			return;
		}
		try {
			requestStream.onNext(AgentMessage.newBuilder()
					.setAgentId(agentId)
					.setHeartbeat(buildHeartbeat())
					.build());
		}
		catch (Exception e) {
			LOGGER.error("heartbeat failed.", e);
			connected.set(false);
			scheduleReconnect(1);
		}
	}

	final SystemInfo systemInfo = new SystemInfo();

	final CentralProcessor cpu = systemInfo.getHardware().getProcessor();

	volatile long[] prevTicks = cpu.getSystemCpuLoadTicks();

	private Heartbeat buildHeartbeat() {
		GlobalMemory globalMemory = systemInfo.getHardware().getMemory();
		double cpuUsage = cpu.getSystemCpuLoadBetweenTicks(prevTicks);
		prevTicks = cpu.getSystemCpuLoadTicks();
		return Heartbeat.newBuilder()
				.setTimestamp(System.currentTimeMillis())
				.setFreeMemory(globalMemory.getAvailable())
				.setCpuUsage(cpuUsage)
				.build();
	}

	@Override
	public void sendProgress() {
		for (DownloadTaskContext task : registry.all()) {
			if (task.status().get() == TaskStatus.RUNNING) {
				requestStream.onNext(AgentMessage.newBuilder().setProgress(
								TransferProcess.newBuilder()
										.setTaskId(task.taskId())
										.setTransferred(task.transferred().get())
										.build())
						.build());
			}
		}
	}

	@Override
	public void sendTaskResult() {

	}

	@PreDestroy
	public void stop() {
		scheduler.shutdownNow();
		if (requestStream != null) {
			requestStream.onCompleted();
		}
		if (channel != null) {
			channel.shutdownNow();
		}
	}

	private void scheduleReconnect(long delay) {
		if (!reconnectScheduled.compareAndSet(false, true)) {
			return;
		}
		scheduler.schedule(() -> {
			try {
				connect();
			}
			finally {
				reconnectScheduled.set(false);
			}
		}, delay, TimeUnit.SECONDS);
	}
}