package com.example.server.entity;

import com.example.grpc.proto.artifact.distribution.TaskStatus;

/**
 *
 * @author carl
 * @date 6/15/26 9:42 PM
 */
public record TaskProgressEvent(

		Integer taskId,

		long transferredSize,

		TaskStatus status
) {
}