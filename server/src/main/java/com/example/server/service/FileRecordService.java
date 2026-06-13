package com.example.server.service;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.example.server.entity.FileRecord;
import com.example.server.repository.FileRecordRepository;
import com.example.server.support.ChunkWriter;
import com.example.server.support.LocalFileWriter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;

/**
 *
 * @author carl
 * @date 6/10/26 11:04 PM
 */
@Service
public class FileRecordService {

	private FileRecordRepository fileRecordRepository;

	private ChunkWriter chunkWriter;

	private Path uploadedDir;

	public FileRecordService(FileRecordRepository fileRecordRepository, @Value("${files.uploaded.dir}") String uploadedDir) {
		this.fileRecordRepository = fileRecordRepository;
		this.uploadedDir = Paths.get(uploadedDir);
		this.chunkWriter = new LocalFileWriter(this.uploadedDir);
	}

	public Mono<FileRecord> saveToDisk(Flux<FilePart> files) {
		return files.flatMap(filePart -> {
					String filename = Paths.get(filePart.filename()).getFileName().toString();
					return chunkWriter.write(filename, 0, filePart.content()).thenReturn(filename);
				})
				.collectList()
				.flatMap(filenames -> {
					FileRecord record = new FileRecord();
					record.setFileNames(filenames);
					return fileRecordRepository.save(record);
				}).onErrorResume(Mono::error);
	}

	public Mono<FileRecord> findById(int id) {
		return fileRecordRepository.findById(id);
	}

	public Mono<FileRecord> save(FileRecord fileRecord) {
		return fileRecordRepository.save(fileRecord);
	}
}