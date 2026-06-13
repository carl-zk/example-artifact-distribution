package com.example.server.entity;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.springframework.data.annotation.Id;

/**
 *
 * @author carl
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FileRecord {
	@Id
	private int id;

	private String fileName;

	private String filePath;

	private long fileSize;

	private String sha256;

	private List<String> fileNames;
}