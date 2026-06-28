package com.example.agent.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.agent.config.ConfigProps;
import com.example.grpc.proto.artifact.distribution.AgentMessage;
import com.example.grpc.proto.artifact.distribution.ControlPlaneGrpc;
import com.example.grpc.proto.artifact.distribution.Heartbeat;
import com.example.grpc.proto.artifact.distribution.RegisterRequest;
import com.example.grpc.proto.artifact.distribution.ServerMessage;
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
import oshi.software.os.OperatingSystem;

import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
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

	final ConfigProps configProps;

	final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
			Thread.ofPlatform().name("scheduler-", 0).factory()
	);

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
				LOGGER.info("new task: {}", msg.getAssignTask().getTaskId());
				var task = msg.getAssignTask();
				registry.enqueue(task);
				LOGGER.info("task enqueued taskId={}, fileName={}", task.getTaskId(), task.getFileName());
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
		String ip = configProps.getPodIp();
		String hostName = configProps.getPodIp();
		OperatingSystem os = systemInfo.getOperatingSystem();
		String osName = os.getFamily();
		String arch = System.getProperty("os.arch");
		requestStream.onNext(AgentMessage.newBuilder()
				.setAgentId(configProps.getAgentId())
				.setRegister(RegisterRequest.newBuilder()
						.setEnv(configProps.getEnv())
						.setNamespace(configProps.getNamespace())
						.setAgentId(configProps.getAgentId())
						.setHostname(hostName)
						.setIp(ip)
						.setOs(osName)
						.setArch(arch)
						.setVersion("alpha-1")
						.build())
				.build());
	}

	@Override
	public synchronized void reconnect() {
	}

	@Scheduled(fixedRate = 30_000)
	@Override
	public void sendHeartbeat() {
		LOGGER.info("send heartbeat in thread {}", Thread.currentThread().getName());
		if (!connected.get() || requestStream == null) {
			return;
		}
		try {
			requestStream.onNext(AgentMessage.newBuilder()
					.setAgentId(configProps.getAgentId())
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
	}

	@EventListener
	public void handleProgressEvent(TransferProcess process) {
		requestStream.onNext(AgentMessage.newBuilder().setProgress(
				process
		).build());
		LOGGER.info("process progress: {}, thread {}", process, Thread.currentThread().getName());
	}

	@Override
	public void sendTaskResult() {

	}

	@PreDestroy
	public void stop() {
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