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

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OzoneManager;

import java.io.IOException;

import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_IN_PROGRESS;

/**
 * UpgradeFinalizer implementation for the Ozone Manager service.
 */
public class OMUpgradeFinalizer extends BasicUpgradeFinalizer<OzoneManager,
    OMLayoutVersionManager> {

  public OMUpgradeFinalizer(OMLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public void preFinalizeUpgrade(OzoneManager ozoneManager)
      throws IOException {
    if (!ozoneManager.getVersionManager().getUpgradeState().equals(FINALIZATION_IN_PROGRESS)) {
      ozoneManager.getFinalizationManager().getFinalizationStateManager().addFinalizingMark();
      ozoneManager.getVersionManager().setUpgradeState(FINALIZATION_IN_PROGRESS);
    }
  }

  @Override
  public void finalizeLayoutFeature(LayoutFeature layoutFeature,
                                    OzoneManager om) throws UpgradeException {
    try {
      om.getFinalizationManager().getFinalizationStateManager()
          .finalizeLayoutFeature(layoutFeature.layoutVersion());
    } catch (IOException ex) {
      throw new UpgradeException(ex,
          UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED);
    }
  }


  void replicatedFinalizationSteps(OMLayoutFeature layoutFeature, OzoneManager om) throws UpgradeException {
    super.finalizeLayoutFeature(layoutFeature,
        layoutFeature.action(LayoutFeature.UpgradeActionType.ON_FINALIZE),
        om.getOmStorage());
  }

  @Override
  public void postFinalizeUpgrade(OzoneManager ozoneManager)
      throws IOException {
    if (ozoneManager.getVersionManager().getUpgradeState().equals(FINALIZATION_IN_PROGRESS)) {
      ozoneManager.getFinalizationManager().getFinalizationStateManager().removeFinalizingMark();
      ozoneManager.getVersionManager().setUpgradeState(FINALIZATION_DONE);
    }
  }

  public void runPrefinalizeStateActions(Storage storage, OzoneManager om)
      throws IOException {
    super.runPrefinalizeStateActions(
        lf -> ((OMLayoutFeature) lf)::action, storage, om);
  }
}
