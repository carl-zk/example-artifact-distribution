package com.example.agent.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.grpc.proto.artifact.distribution.AgentMessage;
import com.example.grpc.proto.artifact.distribution.ControlPlaneGrpc;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import com.example.grpc.proto.artifact.distribution.Heartbeat;
import com.example.grpc.proto.artifact.distribution.RegisterRequest;
import com.example.grpc.proto.artifact.distribution.ServerMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

	final ExecutorService taskExecutor;

	volatile ManagedChannel channel;

	volatile StreamObserver<AgentMessage> requestStream;

	final AtomicBoolean connected = new AtomicBoolean(false);

	final AtomicBoolean connecting = new AtomicBoolean(false);

	final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);

	final AgentDownloader downloader;

	final DownloadTaskDispatcher dispatcher;

	@PostConstruct
	@Override
	public void start() {
		this.channel = ManagedChannelBuilder
				.forAddress("localhost", 9090)
				.enableRetry()
				.maxRetryAttempts(1)
				.keepAliveTime(5, TimeUnit.MINUTES)
				.keepAliveTimeout(20, TimeUnit.SECONDS)
				.keepAliveWithoutCalls(false)
				.maxInboundMessageSize(32 * 1024 * 1024)
				.usePlaintext()
				.build();
		LOGGER.info("Connect to server successfully, channel={}", System.identityHashCode(channel));
		scheduler.scheduleAtFixedRate(this::sendHeartbeat,
				30,
				30,
				TimeUnit.SECONDS);
		connect();
	}

	private void connect() {
		if (!connecting.compareAndSet(false, true)) {
			return;
		}
		try {
			ControlPlaneGrpc.ControlPlaneStub stub = ControlPlaneGrpc.newStub(channel);
			requestStream = stub.connect(streamObserver);
			sendRegister();
			connected.set(true);
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
				taskExecutor.submit(() -> {
					try {
						LOGGER.info("start download");
						downloader.download(msg.getAssignTask().getSource(), FileTransferServiceGrpc.newStub(channel), FileTransferServiceGrpc.newBlockingStub(channel));
						LOGGER.info("download rpc submitted");
					}
					catch (Exception e) {
						LOGGER.error("download error", e);
					}
				});
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

	}

	@Override
	public void sendTaskResult() {

	}

	@PreDestroy
	public void stop() {
		scheduler.shutdownNow();
		taskExecutor.shutdownNow();
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