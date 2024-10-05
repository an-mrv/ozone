package org.apache.hadoop.ozone.om.upgrade;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;

public interface FinalizationManager {
  FinalizationStateManager getFinalizationStateManager();

  StatusAndMessages finalizeUpgrade(String upgradeClientID)
      throws IOException;

  StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException;

  void runPrefinalizeStateActions() throws IOException;

  void reinitialize(Table<String, String> finalizationStore) throws IOException;

  void onLeaderReady();
}

