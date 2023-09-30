package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.BigDataClient;
import com.db.dataplatform.techtest.server.exception.EntityNotFoundException;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;
    private final BigDataClient bigDataClient;

    /**
     * @param envelope request to persist
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) throws NoSuchAlgorithmException, HadoopClientException {

        String checksum  = checksum(envelope.getDataBody().getDataBody());
        boolean checksumMatch = checksum.equals(envelope.getChecksum());
        log.info("Checksum matching status is {}", checksumMatch);

        if (checksumMatch) {
            // Save to persistence.
            persist(envelope);
            log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());

            pushBigData(envelope);
            log.info("Pushed data to Hadoop server");
        }
        return checksumMatch;
    }

    @Override
    public List<DataEnvelope> getDataByBlockType(BlockTypeEnum blockType) {
        List<DataBodyEntity> entityList = dataBodyServiceImpl.getDataByBlockType(blockType);
        List<DataEnvelope> dataEnvelopes = entityList.stream().map(this::map).collect(Collectors.toList());
     
        log.info("Retrieved data with size: {}", dataEnvelopes.size());
        return dataEnvelopes;
    }

    @Override
    public boolean updateDataByName(String name, BlockTypeEnum newBlockType) throws EntityNotFoundException {
        DataBodyEntity bodyEntity = dataBodyServiceImpl.getDataByBlockName(name)
                .orElseThrow(EntityNotFoundException::new);
        bodyEntity.getDataHeaderEntity().setBlocktype(newBlockType);
        dataBodyServiceImpl.saveDataBody(bodyEntity);
        log.info("Updated BlockType for the name: {}", name);

        return true;
    }

    private void pushBigData(DataEnvelope envelope) throws HadoopClientException {
        bigDataClient.pushBigData(envelope);
    }

    private String checksum(String data) throws NoSuchAlgorithmException {
        byte[] hash = MessageDigest.getInstance("MD5").digest(data.getBytes());
        return Hex.encodeHexString(hash);
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    private DataEnvelope map(DataBodyEntity entity) {
        return new DataEnvelope(
                new DataHeader(entity.getDataHeaderEntity().getName(), entity.getDataHeaderEntity().getBlocktype()),
                new DataBody(entity.getDataBody()), null
        );
    }
}
