package com.example.agent;

import com.example.agent.core.AgentDownloader;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *
 * @author carl
 * @date 6/12/26 7:00 AM
 */
@SpringBootApplication
public class AgentApplication implements CommandLineRunner {
	public static void main(String[] args) {
		SpringApplication.run(AgentApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		AgentDownloader downloader = new AgentDownloader("localhost", 9090);
		downloader.download("1");
	}
}