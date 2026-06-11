package com.example.artifact.distribution.server.server.service;

import com.example.artifact.distribution.server.server.entity.FileRecord;
import com.example.artifact.distribution.server.server.repository.FileRecordRepository;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;

/**
 *
 * @author carl
 * @date 6/10/26 11:04 PM
 */
@Service
public class FileRecordService {

	private FileRecordRepository fileRecordRepository;

	public FileRecordService(FileRecordRepository fileRecordRepository) {
		this.fileRecordRepository = fileRecordRepository;
	}

	public Mono<FileRecord> findById(int id) {
		return fileRecordRepository.findById(id);
	}

	public Mono<FileRecord> save(FileRecord fileRecord) {
		return fileRecordRepository.save(fileRecord);
	}
}