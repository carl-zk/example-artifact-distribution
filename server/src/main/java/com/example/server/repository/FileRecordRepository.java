package com.example.server.repository;

/**
 *
 * @author carl
 */

import com.example.server.entity.FileRecord;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FileRecordRepository extends R2dbcRepository<FileRecord, Integer> {
}