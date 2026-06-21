package com.example.agent.config;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.SimpleAsyncTaskScheduler;

/**
 *
 * @author carl
 * @date 6/13/26 5:25 PM
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties
public class AppConfig {

	@Bean
	public ManagedChannel managedChannel(ConfigProps configProps) {
		return ManagedChannelBuilder
				.forAddress(configProps.remoteServerHost, configProps.remoteServerPort)
				.enableRetry()
				.maxRetryAttempts(10)
				.keepAliveTime(5, TimeUnit.MINUTES)
				.keepAliveTimeout(20, TimeUnit.SECONDS)
				.keepAliveWithoutCalls(false)
				.maxInboundMessageSize((int) configProps.maxInboundMessageSize.toBytes())
				.usePlaintext()
				.build();
	}

	@Bean
	public Executor executor() {
		return Executors.newVirtualThreadPerTaskExecutor();
	}

	@Bean
	public ApplicationEventMulticaster applicationEventMulticaster() {
		SimpleApplicationEventMulticaster multicaster = new SimpleApplicationEventMulticaster();
		// avoid @Async at all
		ThreadFactory threadFactory = Thread.ofVirtual()
				.name("event-vt-", 0)
				.factory();
		multicaster.setTaskExecutor(Executors.newThreadPerTaskExecutor(threadFactory));
		return multicaster;
	}

	@Bean
	public TaskScheduler taskScheduler() {
		// for @Scheduled
		SimpleAsyncTaskScheduler scheduler = new SimpleAsyncTaskScheduler();
		scheduler.setVirtualThreads(true);
		scheduler.setThreadNamePrefix("task-vt-");
		return scheduler;
	}
}