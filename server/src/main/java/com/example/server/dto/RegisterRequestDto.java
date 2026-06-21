package com.example.server.dto;

/**
 *
 * @author carl
 * @date 6/20/26 8:50 PM
 */
public record RegisterRequestDto(
		String agentId,
		String ip,
		String hostname,
		String os,
		String arch,
		String version,
		String env,
		String namespace
) {
}