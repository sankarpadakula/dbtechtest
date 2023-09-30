package com.db.dataplatform.techtest.server.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class ErrorMessage {

    private Instant timestamp;
    private HttpStatus status;
    private String message;
    private List errors;
}
