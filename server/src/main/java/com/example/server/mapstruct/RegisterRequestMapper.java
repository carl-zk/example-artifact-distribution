package com.example.server.mapstruct;

import com.example.grpc.proto.artifact.distribution.RegisterRequest;
import com.example.server.dto.RegisterRequestDto;
import org.mapstruct.Mapper;

/**
 *
 * @author carl
 * @date 6/20/26 8:53 PM
 */
@Mapper(componentModel = "spring")
public interface RegisterRequestMapper {
	RegisterRequestDto toDto(RegisterRequest request);

	RegisterRequest toEntity(RegisterRequestDto dto);
}