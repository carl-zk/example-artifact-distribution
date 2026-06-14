package com.example.agent.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 *
 * @author carl
 * @date 6/13/26 5:25 PM
 */
@Configuration
@EnableScheduling
public class AppConfig {

	@Bean
	public ManagedChannel managedChannel() {
		return ManagedChannelBuilder
				.forAddress("localhost", 9090)
				.enableRetry()
				.maxRetryAttempts(1)
				.keepAliveTime(5, TimeUnit.MINUTES)
				.keepAliveTimeout(20, TimeUnit.SECONDS)
				.keepAliveWithoutCalls(false)
				.maxInboundMessageSize(32 * 1024 * 1024)
				.usePlaintext()
				.build();
	}

	@Bean
	public ExecutorService executor() {
		return Executors.newVirtualThreadPerTaskExecutor();
	}
}