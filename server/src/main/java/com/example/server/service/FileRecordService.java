package com.example.server.service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

import com.example.server.config.ConfigProps;
import com.example.server.entity.FileRecord;
import com.example.server.repository.FileRecordRepository;
import com.example.server.support.ChunkWriter;
import com.example.server.support.LocalFileWriter;
import com.example.server.util.HashUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;
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

	private Path storageDir;

	public FileRecordService(FileRecordRepository fileRecordRepository, ConfigProps configProps) {
		this.fileRecordRepository = fileRecordRepository;
		this.storageDir = Paths.get(configProps.getStorageDir());
		this.chunkWriter = new LocalFileWriter(this.storageDir);
	}

	public Flux<FileRecord> saveToDisk(Flux<FilePart> files) {
		return files.flatMap(filePart -> {
			String filename = Paths.get(filePart.filename()).getFileName().toString();

			AtomicLong sizeCounter = new AtomicLong(0);

			Flux<DataBuffer> countedContent = filePart.content()
					.doOnNext(dataBuffer -> { // map() will break buffer lifecycle and leading memory leak !!!
						sizeCounter.addAndGet(dataBuffer.readableByteCount());
					});

			return chunkWriter.write(filename, 0, countedContent)
					.then(Mono.defer(() -> {
						Path path = storageDir.resolve(filename);
						FileRecord fileRecord = FileRecord.builder()
								.fileName(filename)
								.filePath(path.toString())
								.fileSize(sizeCounter.get())
								.sha256(HashUtil.fileSha256(path))
								.build();
						return Mono.just(fileRecord);
					})).flatMap(fileRecordRepository::save);
		});
	}

	public Mono<FileRecord> findById(int id) {
		return fileRecordRepository.findById(id);
	}

	public Mono<FileRecord> save(FileRecord fileRecord) {
		return fileRecordRepository.save(fileRecord);
	}

	public Flux<FileRecord> findAll() {
		return fileRecordRepository.findAll();
	}
}