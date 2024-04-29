package ru.turbogoose.cca.backend.components.storage.info;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Getter
public class StorageInfoHelper {
    private final StorageInfoRepository storageInfoRepository;

    public void setStatusAndSave(String storageId, StorageStatus status) {
        storageInfoRepository.updateStorageStatusById(storageId, status);
    }

    public void updateStatus(StorageInfo storageInfo) {
        StorageInfo actualStorageInfo = getInfoByStorageIdOrThrow(storageInfo.getStorageId());
        storageInfo.setStatus(actualStorageInfo.getStatus());
    }

    public StorageInfo getInfoByStorageIdOrThrow(String storageId) {
        return storageInfoRepository.getByStorageId(storageId).orElseThrow(
                () -> new IllegalStateException("Storage with id %s not found".formatted(storageId)));
    }

    public boolean hasAnyOfStatuses(String storageId, StorageStatus status, StorageStatus... statuses) {
        return storageInfoRepository.getByStorageId(storageId)
                .filter(storageInfo -> hasAnyOfStatuses(storageInfo, status, statuses))
                .isPresent();
    }

    public boolean hasAnyOfStatuses(StorageInfo storageInfo, StorageStatus status, StorageStatus... statuses) {
        StorageStatus actualStatus = storageInfo.getStatus();
        if (actualStatus == status) {
            return true;
        }
        for (StorageStatus stat : statuses) {
            if (actualStatus == stat) {
                return true;
            }
        }
        return false;
    }
}
