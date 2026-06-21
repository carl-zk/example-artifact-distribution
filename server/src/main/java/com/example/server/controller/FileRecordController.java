package com.example.server.controller;

import com.example.server.entity.FileRecord;
import com.example.server.service.FileRecordService;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author carl
 * @date 6/10/26 10:43 PM
 */
@RequestMapping("/api/files")
@RestController
@RequiredArgsConstructor
public class FileRecordController {
	private final FileRecordService fileRecordService;

	@GetMapping("/{id}")
	public Mono<FileRecord> getFileRecordById(@PathVariable("id") int id) {
		return fileRecordService.findById(id)
				.onErrorResume(Mono::error);
	}

	@PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public Flux<FileRecord> uploadFile(@RequestPart("files") Flux<FilePart> files) {
		return fileRecordService.saveToDisk(files);
	}

	@GetMapping
	public Flux<FileRecord> getFileRecords() {
		return fileRecordService.findAll();
	}
}