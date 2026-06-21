package com.example.server.entity;

import java.time.LocalDateTime;

import com.example.grpc.proto.artifact.distribution.TaskStatus;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.springframework.data.annotation.Id;

/**
 *
 * @author carl
 * @date 6/19/26 12:58 PM
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Task {

	@Id
	Integer id;

	String agentId;

	String agentIp;

	Integer fileId;

	String fileName;

	Long fileSize;

	String saveToDir;

	@Enumerated(EnumType.STRING)
	TaskStatus status;

	LocalDateTime createdTime;
}