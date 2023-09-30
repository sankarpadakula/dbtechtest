package com.db.dataplatform.techtest.service;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.BigDataClient;
import com.db.dataplatform.techtest.server.exception.EntityNotFoundException;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.mapper.ServerMapperConfiguration;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.component.impl.ServerImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.modelmapper.ModelMapper;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ServerServiceTests {

    @Mock
    private DataBodyService dataBodyServiceImplMock;

    private DataBodyEntity expectedDataBodyEntity;
    private DataEnvelope testDataEnvelope;

    private Server server;

    @Mock
    private BigDataClient bigDataClient;


    @Before
    public void setup() {
        ServerMapperConfiguration serverMapperConfiguration = new ServerMapperConfiguration();
        ModelMapper modelMapper = serverMapperConfiguration.createModelMapperBean();

        testDataEnvelope = createTestDataEnvelopeApiObject();
        expectedDataBodyEntity = modelMapper.map(testDataEnvelope.getDataBody(), DataBodyEntity.class);
        expectedDataBodyEntity.setDataHeaderEntity(modelMapper.map(testDataEnvelope.getDataHeader(), DataHeaderEntity.class));

        server = new ServerImpl(dataBodyServiceImplMock, modelMapper, bigDataClient);
    }

    @Test
    public void shouldSaveDataEnvelopeAsExpected() throws NoSuchAlgorithmException, IOException, HadoopClientException {
        boolean success = server.saveDataEnvelope(testDataEnvelope);

        assertThat(success).isTrue();
        verify(dataBodyServiceImplMock, times(1)).saveDataBody(eq(expectedDataBodyEntity));
    }

    @Test
    public void shouldNotSaveUnmatchedChecksumDataEnvelope() throws NoSuchAlgorithmException, IOException, HadoopClientException {
        DataEnvelope dataEnvelope = new DataEnvelope(testDataEnvelope.getDataHeader(), testDataEnvelope.getDataBody(),
                "");

        boolean success = server.saveDataEnvelope(dataEnvelope);
        assertThat(success).isFalse();

        verify(dataBodyServiceImplMock, times(0)).saveDataBody(eq(expectedDataBodyEntity));
    }

    @Test
    public void shouldThroughBigDataExceptionWhilePush() throws HadoopClientException {
        doThrow(HadoopClientException.class).when(bigDataClient).pushBigData(testDataEnvelope);
        assertThatThrownBy(() -> server.saveDataEnvelope(testDataEnvelope))
                .isInstanceOf(HadoopClientException.class);

        verify(dataBodyServiceImplMock, times(1)).saveDataBody(eq(expectedDataBodyEntity));
        verify(bigDataClient, times(1)).pushBigData(eq(testDataEnvelope));
    }

    @Test
    public void shouldUpdateDataEnvelopAsExpected() throws EntityNotFoundException {

        String name = testDataEnvelope.getDataHeader().getName();
        when(dataBodyServiceImplMock.getDataByBlockName(name)).thenReturn(Optional.of(expectedDataBodyEntity));
        boolean success = server.updateDataByName(name, BlockTypeEnum.BLOCKTYPEB);
        assertThat(success).isTrue();
        verify(dataBodyServiceImplMock, times(1)).saveDataBody(any());
    }

    @Test
    public void shouldThrowExceptionForInvalidName() {

        String name = "ABC";
        when(dataBodyServiceImplMock.getDataByBlockName(name)).thenReturn(Optional.empty());
        assertThatThrownBy(() -> server.updateDataByName(name, BlockTypeEnum.BLOCKTYPEB))
                .isInstanceOf(EntityNotFoundException.class);

        verify(dataBodyServiceImplMock, times(0)).saveDataBody(any());
    }

    @Test
    public void shouldQueryDataAsExpected() {
        when(dataBodyServiceImplMock.getDataByBlockType(eq(BlockTypeEnum.BLOCKTYPEB)))
                .thenReturn(Collections.singletonList(expectedDataBodyEntity));
        List<DataEnvelope> dataEnvelopByBlocktype = server.getDataByBlockType(BlockTypeEnum.BLOCKTYPEB);
        assertThat(dataEnvelopByBlocktype).hasSize(1);
        assertThat(dataEnvelopByBlocktype.get(0).getDataBody().getDataBody()).isEqualTo(TestDataHelper.DUMMY_DATA);
        verify(dataBodyServiceImplMock, times(1))
                .getDataByBlockType(eq(BlockTypeEnum.BLOCKTYPEB));
    }

}
