package org.apache.hadoop.ozone.om.request.upgrade;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMFinalizeLayoutFeatureResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.FinalizeLayoutFeature;

public class OMFinalizeLayoutFeatureRequest extends OMClientRequest  {
    private static final Logger LOG =
            LoggerFactory.getLogger(OMFinalizeLayoutFeatureRequest.class);

    public OMFinalizeLayoutFeatureRequest(OzoneManagerProtocolProtos.OMRequest omRequest) {
        super(omRequest);
    }

    @Override
    public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
        LOG.trace("Request: {}", getOmRequest());
        AuditLogger auditLogger = ozoneManager.getAuditLogger();
        OzoneManagerProtocolProtos.OMResponse.Builder responseBuilder =
                OmResponseUtil.getOMResponseBuilder(getOmRequest());
        responseBuilder.setCmdType(FinalizeLayoutFeature);
        OMClientResponse response = null;
        Exception exception = null;

        try {
            int lv = (int) getOmRequest().getLayoutVersion().getVersion();
            ozoneManager.getFinalizationManager().getFinalizationStateManager().finalizeLayoutFeatureLocal(lv);

            OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
            int lV = ozoneManager.getVersionManager().getMetadataLayoutVersion();
            omMetadataManager.getMetaTable().addCacheEntry(
                    new CacheKey<>(LAYOUT_VERSION_KEY),
                    CacheValue.get(termIndex.getIndex(), String.valueOf(lV)));

            OzoneManagerProtocolProtos.FinalizeLayoutFeatureResponse omResponse =
                    OzoneManagerProtocolProtos.FinalizeLayoutFeatureResponse.newBuilder()
                            .build();
            responseBuilder.setFinalizeLayoutFeatureResponse(omResponse);
            response = new OMFinalizeLayoutFeatureResponse(responseBuilder.build(),
                    ozoneManager.getVersionManager().getMetadataLayoutVersion());
            LOG.trace("Returning response: {}", response);
        } catch (IOException e) {
            exception = e;
            response = new OMFinalizeLayoutFeatureResponse(
                    createErrorOMResponse(responseBuilder, e), -1);
        }

        markForAudit(auditLogger, buildAuditMessage(OMAction.FINALIZE_LAYOUT_FEATURE,
                new HashMap<>(), exception, null));
        return response;
    }
}
