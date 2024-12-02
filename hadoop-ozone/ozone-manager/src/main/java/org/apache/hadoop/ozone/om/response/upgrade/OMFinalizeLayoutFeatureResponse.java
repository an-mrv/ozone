package org.apache.hadoop.ozone.om.response.upgrade;

import java.io.IOException;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;

//подумать про аннотацию как в OMFinalizeUpgradeResponse
public class OMFinalizeLayoutFeatureResponse extends OMClientResponse {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMFinalizeLayoutFeatureResponse.class);

    private int layoutVersionToWrite = -1;

    public OMFinalizeLayoutFeatureResponse(
            OzoneManagerProtocolProtos.OMResponse omResponse,
            int layoutVersionToWrite) {
        super(omResponse);
        this.layoutVersionToWrite = layoutVersionToWrite;
    }

    @Override
    protected void addToDBBatch(OMMetadataManager omMetadataManager,
                                BatchOperation batchOperation) throws IOException {
        if (layoutVersionToWrite != -1) {
            LOG.info("Layout version to persist to DB : {}", layoutVersionToWrite);
            omMetadataManager.getMetaTable().putWithBatch(batchOperation,
                    LAYOUT_VERSION_KEY,
                    String.valueOf(layoutVersionToWrite));
        }
    }
}

