package dkarag.spark.dtos;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

@Data
public class ReadTableOnDateRangeDTO {
    private String tableName;
    private String dateColumnName;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private String startDate;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private String endDate;
}
