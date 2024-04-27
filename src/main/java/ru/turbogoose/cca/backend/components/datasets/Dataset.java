package ru.turbogoose.cca.backend.components.datasets;

import jakarta.persistence.*;
import lombok.*;
import ru.turbogoose.cca.backend.components.labels.Label;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageMode;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
@Entity
@Table(name="datasets")
public class Dataset {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    @Column(nullable = false)
    private String name;
    @Column(nullable = false)
    private Long size;
    private Integer totalRows; // TODO: Long?
    @Column(nullable = false)
    private LocalDateTime created;
    private LocalDateTime lastUpdated;

    @OneToMany(mappedBy = "dataset", cascade = CascadeType.ALL, orphanRemoval = true)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private List<Label> labels = new ArrayList<>();

    @OneToMany(mappedBy = "dataset", fetch = FetchType.EAGER, cascade = CascadeType.ALL, orphanRemoval = true)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private List<StorageInfo> storages = new ArrayList<>();

    public void addStorage(StorageInfo storageInfo) {
        if (storageInfo != null) {
            getStorages().add(storageInfo);
            storageInfo.setDataset(this);
        }
    }

    public Optional<StorageInfo> getStorage(StorageMode mode) {
        return getStorages().stream().filter(s -> s.getMode() == mode).findFirst();
    }

    public void removeStorage(StorageInfo storageInfo) {
        if (storageInfo != null) {
            getStorages().remove(storageInfo);
            storageInfo.setDataset(null);
        }
    }
}