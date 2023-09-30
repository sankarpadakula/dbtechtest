package com.db.dataplatform.techtest.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.BigDataClient;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObject;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BigDataClientTest {

    private DataEnvelope testDataEnvelope;

    private BigDataClient bigDataClient;

    @Mock
    RestTemplate restTemplate;

    @Before
    public void setup() {
        testDataEnvelope = createTestDataEnvelopeApiObject();

        bigDataClient = new BigDataClient(restTemplate);

    }

    @Test
    public void shouldPushDataToHadoopAsExpected() throws HadoopClientException {
        bigDataClient.pushBigData(testDataEnvelope);

        verify(restTemplate, times(1))
                .postForEntity(anyString(), eq(testDataEnvelope), eq(String.class));
    }

    @Test
    public void shouldPushDataToHadoopOnThirdTimeAsExpected() {
        when(restTemplate.postForEntity(anyString(), eq(testDataEnvelope), eq(String.class)))
                .thenThrow(HttpServerErrorException.GatewayTimeout.class);

        assertThatThrownBy(() -> bigDataClient.pushBigData(testDataEnvelope))
                .isInstanceOf(HttpStatusCodeException.class);

        verify(restTemplate, times(1))
                .postForEntity(anyString(), eq(testDataEnvelope), eq(String.class));
    }
}
