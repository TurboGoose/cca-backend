package ru.turbogoose.cca.backend.components.storage.info;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import ru.turbogoose.cca.backend.components.datasets.Dataset;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name="storages")
public class StorageInfo extends InternalStorageInfo {
    @Id
    private Integer id;
    private StorageMode mode;
    @ManyToOne
    private Dataset dataset;

    public StorageInfo(InternalStorageInfo info, StorageMode mode) {
        super(info);
        this.mode = mode;
    }
}
