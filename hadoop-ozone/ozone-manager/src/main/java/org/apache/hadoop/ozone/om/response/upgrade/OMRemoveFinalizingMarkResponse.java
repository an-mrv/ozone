package org.apache.hadoop.ozone.om.response.upgrade;

import java.io.IOException;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OMRemoveFinalizingMarkResponse extends OMClientResponse {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMRemoveFinalizingMarkResponse.class);

    public OMRemoveFinalizingMarkResponse(
            OzoneManagerProtocolProtos.OMResponse omResponse) {
        super(omResponse);
    }

    @Override
    protected void addToDBBatch(OMMetadataManager omMetadataManager,
                                BatchOperation batchOperation) throws IOException {
        omMetadataManager.getMetaTable().deleteWithBatch(batchOperation,
                OzoneConsts.FINALIZING_KEY);
        LOG.info("Finalizing mark removed from DB.");
    }
}
