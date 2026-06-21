package com.example.agent.config;


import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;

/**
 *
 * @author carl
 * @date 6/20/26 5:01 PM
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "configs")
public class ConfigProps {
	String env = "default";

	String namespace = "default";

	String agentId = UUID.randomUUID().toString();

	String remoteServerHost = "127.0.0.1";

	int remoteServerPort = 8080;

	DataSize maxInboundMessageSize = DataSize.ofMegabytes(4);
}