package dkarag.spark.dtos;

import lombok.Data;

@Data
public class ReadTableOnPartitionsDTO {
    private String tableName;
    private String partitionColumnName;
    private int valueToDivideCountOfRows;
}
