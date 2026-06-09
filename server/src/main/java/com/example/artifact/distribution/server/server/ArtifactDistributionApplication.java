package com.example.artifact.distribution.server.server;

import com.example.grpc.proto.artifact.distribution.FileServiceGrpc;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ArtifactDistributionApplication {

	public static void main(String[] args) {
		SpringApplication.run(ArtifactDistributionApplication.class, args);
	}

	@Bean
	ApplicationRunner runner() {
		return (args) -> {
			FileServiceGrpc f;
			System.out.println("hel" +
					"");
		};
	}
}