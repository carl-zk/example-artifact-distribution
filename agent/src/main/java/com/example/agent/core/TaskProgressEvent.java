package com.example.agent.core;

import com.example.grpc.proto.artifact.distribution.TransferProcess;

/**
 *
 * @author carl
 * @date 6/20/26 2:26 PM
 */
public record TaskProgressEvent(TransferProcess prgress) {
}