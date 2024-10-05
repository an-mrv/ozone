package org.apache.hadoop.ozone.om.upgrade;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinalizationManagerImpl implements FinalizationManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(FinalizationManagerImpl.class);

  private final OzoneManager ozoneManager;
  private final OMUpgradeFinalizer upgradeFinalizer;
  private OMStorage omStorage;
  private final FinalizationStateManager finalizationStateManager;
  private ThreadFactory threadFactory;

  public FinalizationManagerImpl(OzoneManager ozoneManager) throws IOException {
    this.ozoneManager = ozoneManager;
    this.upgradeFinalizer = (OMUpgradeFinalizer) ozoneManager.getUpgradeFinalizer();
    this.omStorage = ozoneManager.getOmStorage();
    this.finalizationStateManager = new FinalizationStateManagerImpl(ozoneManager);;

    String prefix = ozoneManager.getThreadNamePrefix();
    this.threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(prefix + "FinalizationManager-%d")
        .build();
  }

  @Override
  public FinalizationStateManager getFinalizationStateManager() {
    return finalizationStateManager;
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages finalizeUpgrade(
      String upgradeClientID)
      throws IOException {
    return upgradeFinalizer.finalize(upgradeClientID, ozoneManager);
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(String upgradeClientID, boolean takeover, boolean readonly) throws IOException {
    if (readonly) {
      return new UpgradeFinalizer.StatusAndMessages(upgradeFinalizer.getStatus(),
          Collections.emptyList());
    }
    return upgradeFinalizer.reportStatus(upgradeClientID, takeover);
  }

  @Override
  public void runPrefinalizeStateActions() throws IOException {
    upgradeFinalizer.runPrefinalizeStateActions(omStorage, ozoneManager);
  }

  @Override
  public void reinitialize(Table<String, String> finalizationStore) throws IOException {
    finalizationStateManager.reinitialize(finalizationStore);
  }

  @Override
  public void onLeaderReady() {
// Launch a background thread to drive finalization.
    Executors.newSingleThreadExecutor(threadFactory).submit(() -> {
      if (finalizationStateManager.isHasFinalizingMark()) {
        LOG.info("OM became leader. Resuming upgrade finalization.");
        try {
          finalizeUpgrade("resume-finalization-as-leader");
        } catch (IOException ex) {
          ExitUtils.terminate(1,
              "Resuming upgrade finalization failed on OM leader change.",
              ex, true, LOG);
        }
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("OM became leader. No upgrade finalization action required.");
      }
    });
  }

}
