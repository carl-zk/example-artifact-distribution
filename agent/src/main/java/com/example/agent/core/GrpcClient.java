package com.example.agent.core;

import com.example.grpc.proto.artifact.distribution.ControlPlaneGrpc;
import com.example.grpc.proto.artifact.distribution.FileTransferServiceGrpc;
import io.grpc.ManagedChannel;
import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Component;

/**
 *
 * @author carl
 * @date 6/14/26 7:12 PM
 */
@Component
@RequiredArgsConstructor
public class GrpcClient {
	final ManagedChannel channel;

	public FileTransferServiceGrpc.FileTransferServiceStub getFileTransferServiceStub() {
		return FileTransferServiceGrpc.newStub(channel);
	}

	public FileTransferServiceGrpc.FileTransferServiceBlockingStub getFileTransferServiceBlockingStub() {
		return FileTransferServiceGrpc.newBlockingStub(channel);
	}

	public ControlPlaneGrpc.ControlPlaneStub getControlPlaneStub() {
		return ControlPlaneGrpc.newStub(channel);
	}
}