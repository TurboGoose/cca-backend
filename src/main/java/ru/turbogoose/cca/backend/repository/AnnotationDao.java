package ru.turbogoose.cca.backend.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import ru.turbogoose.cca.backend.model.Label;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class AnnotationDao {
    private final NamedParameterJdbcTemplate npjt;

    // TODO: incapsulate map into object
    public Map<Long, List<Label>> getAnnotationsForPage(int datasetId, Pageable pageable) {
        final String SQL = "with dataset_labels as (select * from labels where dataset_id = :DATASET_ID) " +
                "select a.row_num as rowNum, a.label_id as labelId, dl.name as labelName " +
                "from annotations a " +
                "join dataset_labels dl on a.label_id = dl.id " +
                "where row_num between :FROM and :TO " +
                "order by a.row_num";

        SqlParameterSource params = constructParameterSource(datasetId, pageable);
        return npjt.query(SQL, params, new AnnotationResultSetExtractor());
    }

    private SqlParameterSource constructParameterSource(int datasetId, Pageable pageable) {
        long from = pageable.getOffset();
        long to = from + pageable.getPageSize();
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("FROM", from);
        params.addValue("TO", to);
        params.addValue("DATASET_ID", datasetId);
        return params;
    }

    static class AnnotationResultSetExtractor implements ResultSetExtractor<Map<Long, List<Label>>> {
        @Override
        public Map<Long, List<Label>> extractData(ResultSet rs) throws SQLException, DataAccessException {
            Map<Long, List<Label>> result = new HashMap<>();
            while (rs.next()) {
                Label label = Label.builder()
                        .name(rs.getString("labelName"))
                        .id(rs.getInt("labelId"))
                        .build();
                long rowNum = rs.getLong("rowNum");
                result.putIfAbsent(rowNum, new LinkedList<>());
                result.get(rowNum).add(label);
            }
            return result;
        }
    }
}
