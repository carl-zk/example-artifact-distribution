package com.example.agent.core;

/**
 *
 * @author carl
 * @date 6/13/26 6:09 PM
 */
public interface ConnectionManager {
	void start();

	void reconnect();

	void sendHeartbeat();

	void sendProgress();

	void sendTaskResult();
}