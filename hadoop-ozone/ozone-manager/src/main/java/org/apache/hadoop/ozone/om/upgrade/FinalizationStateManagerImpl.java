package org.apache.hadoop.ozone.om.upgrade;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinalizationStateManagerImpl implements FinalizationStateManager {

  @VisibleForTesting
  public static final Logger LOG =
      LoggerFactory.getLogger(FinalizationStateManagerImpl.class);
  private final OzoneManager ozoneManager;
  private final OMLayoutVersionManager versionManager;
  private final ReadWriteLock checkpointLock;
  private volatile boolean hasFinalizingMark;
  private final OMUpgradeFinalizer upgradeFinalizer;
  private final Table<String, String> metaTable;

  public FinalizationStateManagerImpl(OzoneManager ozoneManager) throws IOException {
    this.ozoneManager = ozoneManager;
    this.upgradeFinalizer = (OMUpgradeFinalizer) ozoneManager.getUpgradeFinalizer();
    this.versionManager = ozoneManager.getVersionManager();
    this.checkpointLock = new ReentrantReadWriteLock();
    this.metaTable = ozoneManager.getMetadataManager().getMetaTable();
    initialize();
  }

  private void initialize() throws IOException {
    this.hasFinalizingMark =
        metaTable.isExist(OzoneConsts.FINALIZING_KEY);
  }

  @Override
  public void addFinalizingMark() throws IOException {
    checkpointLock.writeLock().lock();
    try {
      hasFinalizingMark = true;
    } finally {
      checkpointLock.writeLock().unlock();
    }
    metaTable.put(OzoneConsts.FINALIZING_KEY, "");
  }

  @Override
  public void finalizeLayoutFeature(Integer layoutVersion) throws IOException {
    finalizeLayoutFeatureLocal(layoutVersion);
  }

  private void finalizeLayoutFeatureLocal(Integer layoutVersion)
      throws IOException {
    checkpointLock.writeLock().lock();
    try {
      OMLayoutFeature feature =
          (OMLayoutFeature)versionManager.getFeature(layoutVersion);
      upgradeFinalizer.replicatedFinalizationSteps(feature, ozoneManager);
    } finally {
      checkpointLock.writeLock().unlock();
    }

    metaTable.put(OzoneConsts.LAYOUT_VERSION_KEY, String.valueOf(layoutVersion));
  }

  @Override
  public void removeFinalizingMark() throws IOException {
    checkpointLock.writeLock().lock();
    try {
      hasFinalizingMark = false;
    } finally {
      checkpointLock.writeLock().unlock();
    }
    metaTable.delete(OzoneConsts.FINALIZING_KEY);
  }

  @Override
  public void reinitialize(Table<String, String> newFinalizationStore) throws IOException {

  }
}
