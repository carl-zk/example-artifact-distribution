package com.example.server.config;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.example.server.service.FileTransferGrpcService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.r2dbc.spi.ConnectionFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;

/**
 *
 * @author carl
 * @date 6/10/26 11:01 PM
 */
@Configuration
public class AppConfig {

	@Bean
	public ConnectionFactoryInitializer initializer(ConnectionFactory connectionFactory) {
		ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
		initializer.setConnectionFactory(connectionFactory);
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator(new ClassPathResource("db/schema.sql"));
		populator.setContinueOnError(false);
		initializer.setDatabasePopulator(populator);
		return initializer;
	}

	@Bean
	public Server transferServer() {
		Server server = NettyServerBuilder
				.forPort(9090)
				.maxInboundMessageSize(32 * 1024 * 1024)
				.executor(Executors.newFixedThreadPool(32))
				.addService(new FileTransferGrpcService())
				.build();
		try {
			server.start();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				server.shutdown();
				try {
					server.awaitTermination(
							10,
							TimeUnit.SECONDS
					);
				}
				catch (InterruptedException ignored) {}
			}));
			return server;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}