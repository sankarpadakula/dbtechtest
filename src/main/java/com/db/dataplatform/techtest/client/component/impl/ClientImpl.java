package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    @Autowired
    RestTemplate restTemplate;

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        ResponseEntity<Boolean> response = restTemplate.postForEntity(URI_PUSHDATA, dataEnvelope, Boolean.class);
        log.info("Response data {} ", response);
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);
        URI uri = URI_GETDATA.expand(blockType).normalize();
        DataEnvelope[] response = restTemplate.getForObject(uri, DataEnvelope[].class);
        return Arrays.asList(response != null ? response : new DataEnvelope[0]);
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blockType to {} for block with name {}", newBlockType, blockName);
        URI uri = URI_PATCHDATA.expand(blockName, newBlockType).normalize();
        return Boolean.TRUE.equals(restTemplate.patchForObject(uri, null, Boolean.class));
    }


}
