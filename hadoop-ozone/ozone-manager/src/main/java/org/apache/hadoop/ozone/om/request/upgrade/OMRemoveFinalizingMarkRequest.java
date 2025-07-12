package org.apache.hadoop.ozone.om.request.upgrade;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMRemoveFinalizingMarkResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RemoveFinalizingMark;

public class OMRemoveFinalizingMarkRequest extends OMClientRequest {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMRemoveFinalizingMarkRequest.class);

    public OMRemoveFinalizingMarkRequest(OzoneManagerProtocolProtos.OMRequest omRequest) {
        super(omRequest);
    }

    @Override
    public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
        LOG.trace("Request: {}", getOmRequest());
        AuditLogger auditLogger = ozoneManager.getAuditLogger();
        OzoneManagerProtocolProtos.OMResponse.Builder responseBuilder =
                OmResponseUtil.getOMResponseBuilder(getOmRequest());
        responseBuilder.setCmdType(RemoveFinalizingMark);
        OMClientResponse response = null;
        Exception exception = null;

        try {
            ozoneManager.getFinalizationManager().getFinalizationStateManager().removeFinalizingMark();

            OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
            omMetadataManager.getMetaTable().addCacheEntry(
                    new CacheKey<>(OzoneConsts.FINALIZING_KEY),
                    CacheValue.get(termIndex.getIndex()));

            OzoneManagerProtocolProtos.AddFinalizingMarkResponse omResponse =
                    OzoneManagerProtocolProtos.AddFinalizingMarkResponse.newBuilder()
                            .build();
            responseBuilder.setAddFinalizingMarkResponse(omResponse);
            response = new OMRemoveFinalizingMarkResponse(responseBuilder.build());
            LOG.trace("Returning response: {}", response);
        } catch (IOException e) {
            exception = e;
            response = new OMRemoveFinalizingMarkResponse(
                    createErrorOMResponse(responseBuilder, e));
        }

        markForAudit(auditLogger, buildAuditMessage(OMAction.REMOVE_FINALIZING_MARK,
                new HashMap<>(), exception, null));
        return response;
    }

}

