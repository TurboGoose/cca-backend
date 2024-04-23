package ru.turbogoose.cca.backend.components.storage.info;

import jakarta.persistence.MappedSuperclass;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@MappedSuperclass
public class InternalStorageInfo {
    private String storageId;
    private StorageStatus status;

    public InternalStorageInfo(InternalStorageInfo info) {
        this.storageId = info.getStorageId();
        this.status = info.getStatus();
    }

    public boolean isStorageReady() {
        return status == StorageStatus.READY;
    }
}