package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope) throws IOException, NoSuchAlgorithmException;
}
