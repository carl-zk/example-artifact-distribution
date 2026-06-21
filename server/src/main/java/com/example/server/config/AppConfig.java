package com.example.server.config;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.example.server.service.ControlPlane;
import com.example.server.service.FileTransferGrpcService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.r2dbc.spi.ConnectionFactory;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.springframework.web.reactive.config.CorsRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 *
 * @author carl
 * @date 6/10/26 11:01 PM
 */
@Configuration
@EnableConfigurationProperties
public class AppConfig {

	@Bean
	public ConnectionFactoryInitializer initializer(ConnectionFactory connectionFactory) {
		ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
		initializer.setConnectionFactory(connectionFactory);
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator(new ClassPathResource("db/schema.sql"));
		populator.setContinueOnError(false);
		initializer.setDatabasePopulator(populator);
		initializer.afterPropertiesSet();
		return initializer;
	}

	@Bean
	public Server transferServer(ControlPlane controlPlane, FileTransferGrpcService fileTransferGrpcService) {
		Server server = NettyServerBuilder
				.forPort(9090)
				.permitKeepAliveTime(1, TimeUnit.MINUTES)
				.permitKeepAliveWithoutCalls(true)
				.maxInboundMessageSize(32 * 1024 * 1024)
				.flowControlWindow(32 * 1024 * 1024)
				.executor(Executors.newFixedThreadPool(32))
				.addService(fileTransferGrpcService)
				.addService(controlPlane)
				.build();
		try {
			server.start();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				server.shutdown();
				try {
					server.awaitTermination(
							1,
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

	@Bean
	public WebFluxConfigurer corsConfigurer() {
		return new WebFluxConfigurer() {

			@Override
			public void addCorsMappings(CorsRegistry registry) {
				registry.addMapping("/**")
						.allowedOrigins("http://localhost:5173")
						.allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
						.allowedHeaders("*")
						.allowCredentials(true);
			}
		};
	}
}