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
  private final ReadWriteLock lock;
  private volatile boolean hasFinalizingMark;
  private final OMUpgradeFinalizer upgradeFinalizer;
  private Table<String, String> metaTable;

  public FinalizationStateManagerImpl(OzoneManager ozoneManager) throws IOException {
    this.ozoneManager = ozoneManager;
    this.upgradeFinalizer = (OMUpgradeFinalizer) ozoneManager.getUpgradeFinalizer();
    this.versionManager = ozoneManager.getVersionManager();
    this.lock = new ReentrantReadWriteLock();
    this.metaTable = ozoneManager.getMetadataManager().getMetaTable();
    initialize();
  }

  private void initialize() throws IOException {
    this.hasFinalizingMark =
        metaTable.isExist(OzoneConsts.FINALIZING_KEY);
  }

  @Override
  public boolean isHasFinalizingMark() {
    return hasFinalizingMark;
  }

  @Override
  public void addFinalizingMark() throws IOException {
    lock.writeLock().lock();
    try {
      hasFinalizingMark = true;
    } finally {
      lock.writeLock().unlock();
    }
    metaTable.put(OzoneConsts.FINALIZING_KEY, "");
  }

  @Override
  public void finalizeLayoutFeature(Integer layoutVersion) throws IOException {
    finalizeLayoutFeatureLocal(layoutVersion);
  }

  private void finalizeLayoutFeatureLocal(Integer layoutVersion)
      throws IOException {
    lock.writeLock().lock();
    try {
      OMLayoutFeature feature =
          (OMLayoutFeature)versionManager.getFeature(layoutVersion);
      upgradeFinalizer.replicatedFinalizationSteps(feature, ozoneManager);
    } finally {
      lock.writeLock().unlock();
    }

    metaTable.put(OzoneConsts.LAYOUT_VERSION_KEY, String.valueOf(layoutVersion));
  }

  @Override
  public void removeFinalizingMark() throws IOException {
    lock.writeLock().lock();
    try {
      hasFinalizingMark = false;
    } finally {
      lock.writeLock().unlock();
    }
    metaTable.delete(OzoneConsts.FINALIZING_KEY);
  }

  @Override
  public void reinitialize(Table<String, String> newFinalizationStore) throws IOException {
    lock.writeLock().lock();
    try {
      metaTable.close();
      metaTable = newFinalizationStore;
      initialize();

      int dbLayoutVersion = getDBLayoutVersion();
      int currentLayoutVersion = versionManager.getMetadataLayoutVersion();
      if (currentLayoutVersion < dbLayoutVersion) {
        LOG.info("New OM snapshot received with metadata layout version {}, " +
                "which is higher than this OM's metadata layout version {}." +
                "Attempting to finalize current OM to that version.",
            dbLayoutVersion, currentLayoutVersion);
        for (int version = currentLayoutVersion + 1; version <= dbLayoutVersion;
             version++) {
          finalizeLayoutFeatureLocal(version);
        }
      }
    } catch (Exception ex) {
      LOG.error("Failed to reinitialize finalization state", ex);
      throw new IOException(ex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private int getDBLayoutVersion() throws IOException {
    String dbLayoutVersion = metaTable.get(
        OzoneConsts.LAYOUT_VERSION_KEY);
    if (dbLayoutVersion == null) {
      return versionManager.getMetadataLayoutVersion();
    } else {
      try {
        return Integer.parseInt(dbLayoutVersion);
      } catch (NumberFormatException ex) {
        String msg = String.format(
            "Failed to read layout version from OM DB. Found string %s",
            dbLayoutVersion);
        LOG.error(msg, ex);
        throw new IOException(msg, ex);
      }
    }
  }
}
