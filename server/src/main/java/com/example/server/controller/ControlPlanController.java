package com.example.server.controller;

import java.util.ArrayList;
import java.util.List;

import com.example.server.dto.RegisterRequestDto;
import com.example.server.entity.Task;
import com.example.server.mapstruct.RegisterRequestMapper;
import com.example.server.repository.TaskRepository;
import com.example.server.service.ControlPlane;
import com.example.server.service.ProgressEventBus;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author carl
 * @date 6/15/26 9:40 PM
 */
@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
public class ControlPlanController {
	Logger logger = LoggerFactory.getLogger(ControlPlanController.class);

	final ProgressEventBus eventBus;

	final ControlPlane controlPlane;

	final TaskRepository taskRepository;

	final RegisterRequestMapper registerRequestMapper;

	@GetMapping(
			value = "/events",
			produces = MediaType.TEXT_EVENT_STREAM_VALUE
	)
	public Flux<ServerSentEvent<?>> events() {
		return eventBus.flux()
				.doOnSubscribe(i -> logger.info("eventBus subscribe {}", i))
				.doOnCancel(() -> logger.warn("cancelled"))
				.doOnTerminate(() -> logger.warn("terminated"))
				.doFinally(signal -> logger.warn("signal {}", signal))
				.map(event ->
						ServerSentEvent.builder()
								.event("progress")
								.data(event)
								.build()
				);
	}

	@GetMapping("/agents")
	public Flux<RegisterRequestDto> agents() {
		return Flux.fromIterable(controlPlane.listAgentNodes()).map(registerRequestMapper::toDto);
	}

	record AgengtVo(String id, String ip, String hostName, String os, String arch) {
	}

	@PostMapping("/tasks")
	public Flux<Task> createTask(@RequestBody List<Task> taskList) {
		return taskRepository.saveAll(taskList);
	}

	@GetMapping("/tasks")
	public Flux<Task> getTasks() {
		return taskRepository.findAll();
	}

	@PostMapping("/tasks/start")
	public Mono<Void> startTask(@RequestBody List<Integer> taskIds) {
		return controlPlane.publishTaskOn(taskIds);
	}

	List<CreateTaskRequest> db = new ArrayList<>();

	public record CreateTaskRequest(Long fileId, String agentId) {
	}

}