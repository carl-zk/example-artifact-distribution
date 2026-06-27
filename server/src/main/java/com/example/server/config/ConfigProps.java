package com.example.server.config;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/19/26 3:26 PM
 */
@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "configs")
public class ConfigProps {
	String storageDir;

	int transferServerPort;
}