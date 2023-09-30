package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.CircuitBreaker;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

@Component
@Slf4j
@RequiredArgsConstructor
public class BigDataClient {

    @Value("${bigdata.server}")
    public String bigDataServer;

    private final RestTemplate restTemplate;

    @Retryable(backoff = @Backoff(delay = 500, multiplier = 3), include = {HttpStatusCodeException.class})
    @CircuitBreaker
    public void pushBigData(DataEnvelope envelope) throws HadoopClientException {
        log.info("Pushing data to Big Data...");
        restTemplate.postForEntity(bigDataServer + "/pushbigdata", envelope, String.class);

    }

    @Recover
    public void fallback(Throwable e) throws HadoopClientException {
        throw new HadoopClientException("Not able to send data Big data server " + e.getMessage());
    }
}
