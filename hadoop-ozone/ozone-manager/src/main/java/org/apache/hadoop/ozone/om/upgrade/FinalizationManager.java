/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

