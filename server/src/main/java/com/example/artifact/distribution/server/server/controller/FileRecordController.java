package com.example.artifact.distribution.server.server.controller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.example.artifact.distribution.server.server.entity.FileRecord;
import com.example.artifact.distribution.server.server.service.FileRecordService;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Value;
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
@RequestMapping("/files")
@RestController
@RequiredArgsConstructor
public class FileRecordController {
	private final FileRecordService fileRecordService;

	@Value("${files.uploaded.dir}")
	private String uploadedDir;

	@GetMapping("/{id}")
	public Mono getFileRecordById(@PathVariable("id") int id) {
		return fileRecordService.findById(id)
				.onErrorResume(Mono::error);
	}

	@PostMapping("/upload")
	public Mono uploadFile(@RequestPart("files") Flux<FilePart> files) {
		Path uploadDir = Paths.get(uploadedDir);
		return Mono.fromCallable(() -> {
					Files.createDirectories(uploadDir);
					return uploadDir;
				}).thenMany(files)
				.flatMap(filePart -> {
					String filename = Paths.get(filePart.filename()).getFileName().toString();
					Path target = uploadDir.resolve(filename);
					return filePart.transferTo(target).thenReturn(filename);
				})
				.collectList()
				.flatMap(filenames -> {
					FileRecord record = new FileRecord();
					record.setFileNames(filenames);
					return fileRecordService.save(record);
				}).onErrorResume(Mono::error);
	}
}