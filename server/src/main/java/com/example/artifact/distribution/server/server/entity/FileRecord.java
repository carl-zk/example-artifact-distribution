package com.example.artifact.distribution.server.server.entity;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

import org.springframework.data.annotation.Id;

/**
 *
 * @author carl
 */
@Getter
@Setter
public class FileRecord {
	@Id
	private int id;

	private List<String> fileNames;
}