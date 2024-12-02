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
import org.apache.hadoop.ozone.om.response.upgrade.OMAddFinalizingMarkResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.AddFinalizingMark;


public class OMAddFinalizingMarkRequest extends OMClientRequest {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMAddFinalizingMarkRequest.class);

    public OMAddFinalizingMarkRequest(OMRequest omRequest) {
        super(omRequest);
    }

    @Override
    public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
        LOG.trace("Request: {}", getOmRequest());
        AuditLogger auditLogger = ozoneManager.getAuditLogger();
        OMResponse.Builder responseBuilder =
                OmResponseUtil.getOMResponseBuilder(getOmRequest());
        responseBuilder.setCmdType(AddFinalizingMark);
        OMClientResponse response = null;
        Exception exception = null;

        try {
            ozoneManager.getFinalizationManager().getFinalizationStateManager().addFinalizingMark();

            OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
            omMetadataManager.getMetaTable().addCacheEntry(
                    new CacheKey<>(OzoneConsts.FINALIZING_KEY),
                    CacheValue.get(termIndex.getIndex(), ""));

            OzoneManagerProtocolProtos.AddFinalizingMarkResponse omResponse =
                    OzoneManagerProtocolProtos.AddFinalizingMarkResponse.newBuilder()
                            .build();
            responseBuilder.setAddFinalizingMarkResponse(omResponse);
            response = new OMAddFinalizingMarkResponse(responseBuilder.build());
            LOG.trace("Returning response: {}", response);
        } catch (IOException e) {
            exception = e;
            response = new OMAddFinalizingMarkResponse(
                    createErrorOMResponse(responseBuilder, e));
        }

        markForAudit(auditLogger, buildAuditMessage(OMAction.ADD_FINALIZING_MARK,
                new HashMap<>(), exception, null));
        return response;
    }

}

