package com.db.dataplatform.techtest.server.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.time.Instant;
import java.util.Collections;

@ControllerAdvice
public class TechTestExceptionAdvice {

    @ExceptionHandler(EntityNotFoundException.class)
    public ResponseEntity<Object> handleResourceNotFoundException(EntityNotFoundException ex) {

        ErrorMessage err = new ErrorMessage(
                Instant.now(),
                HttpStatus.BAD_REQUEST,
                "Resource Not Found",
                Collections.singletonList(ex.getMessage()));

        return new ResponseEntity<>(err, err.getStatus());
    }

    @ExceptionHandler(HadoopClientException.class)
    public ResponseEntity<Object> handleHadoopClientException(HadoopClientException ex) {

        ErrorMessage err = new ErrorMessage(
                Instant.now(),
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Timeout when pushing data",
                Collections.singletonList(ex.getMessage()));

        return new ResponseEntity<>(err, err.getStatus());
    }
}
