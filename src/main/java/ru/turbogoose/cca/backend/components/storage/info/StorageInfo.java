package ru.turbogoose.cca.backend.components.storage.info;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.turbogoose.cca.backend.components.datasets.Dataset;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name="storages")
public class StorageInfo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(unique = true, nullable = false)
    private String storageId;

    @Enumerated(EnumType.STRING)
    private StorageStatus status;

    @Enumerated(EnumType.STRING)
    private StorageMode mode;

    @ManyToOne
    private Dataset dataset;
}
