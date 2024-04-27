package ru.turbogoose.cca.backend.components.storage.info;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface StorageInfoRepository extends JpaRepository<StorageInfo, Integer> {
    Optional<StorageInfo> getByStorageId(String storageId);

    void deleteByStorageId(String storageId);

    @Modifying(flushAutomatically = true)
    @Query("UPDATE StorageInfo s SET s.status=:status WHERE s.id=:id")
    void updateStorageStatusById(@Param("id") String id,
                                 @Param("status") StorageStatus status);
}
