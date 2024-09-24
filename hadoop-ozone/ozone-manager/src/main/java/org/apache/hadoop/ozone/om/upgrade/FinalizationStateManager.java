package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.hdds.scm.metadata.Replicate;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.Table;

public interface FinalizationStateManager {
    @Replicate
    void addFinalizingMark() throws IOException;

    @Replicate
    void removeFinalizingMark() throws IOException;

    @Replicate
    void finalizeLayoutFeature(Integer layoutVersion)
        throws IOException;

    void reinitialize(Table<String, String> newFinalizationStore)
        throws IOException;

}
