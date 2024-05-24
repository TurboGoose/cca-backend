package ru.turbogoose.cca.backend.components.datasets;

import jakarta.persistence.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import ru.turbogoose.cca.backend.components.labels.Label;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@Entity
@Table(name = "datasets")
public class Dataset {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    @Column(nullable = false)
    private String name;
    @Column(nullable = false)
    private Long size;
    private Long totalRows;
    @Column(nullable = false)
    private LocalDateTime created;
    private String headersInfo;

    @OneToMany(mappedBy = "dataset", cascade = CascadeType.ALL, orphanRemoval = true)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private List<Label> labels = new ArrayList<>();

    @OneToMany(mappedBy = "dataset", fetch = FetchType.EAGER, cascade = CascadeType.ALL, orphanRemoval = true)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private List<StorageInfo> storages = new ArrayList<>();

    public void setLabels(List<Label> labels) {
        this.labels.clear();
        this.labels.addAll(labels);
    }

    public void setStorages(List<StorageInfo> storages) {
        this.storages.clear();
        this.storages.addAll(storages);
    }

    public void addLabel(Label label) {
        if (label != null) {
            labels.add(label);
            label.setDataset(this);
        }
    }

    public void removeLabel(Label label) {
        if (label != null) {
            labels.remove(label);
            label.setDataset(null);
        }
    }

    public void addStorage(StorageInfo storageInfo) {
        if (storageInfo != null) {
            storages.add(storageInfo);
            storageInfo.setDataset(this);
        }
    }

    public void removeStorage(StorageInfo storageInfo) {
        if (storageInfo != null) {
            storages.remove(storageInfo);
            storageInfo.setDataset(null);
        }
    }
}