package com.example.server.repository;

import com.example.server.entity.Task;

import org.springframework.data.r2dbc.repository.R2dbcRepository;

/**
 *
 * @author carl
 * @date 6/19/26 1:01 PM
 */
public interface TaskRepository extends R2dbcRepository<Task, Integer> {
}