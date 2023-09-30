package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.EntityNotFoundException;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope) throws IOException, NoSuchAlgorithmException, HadoopClientException;

    List<DataEnvelope> getDataByBlockType(BlockTypeEnum blockType);

    boolean updateDataByName(String name, BlockTypeEnum newBlockType) throws EntityNotFoundException;
}
