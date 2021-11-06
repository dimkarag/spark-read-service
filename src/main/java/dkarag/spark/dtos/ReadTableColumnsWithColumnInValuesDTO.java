package dkarag.spark.dtos;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ReadTableColumnsWithColumnInValuesDTO {
    private String tableName;
    private List<String> columnNames = new ArrayList<>();
    private String columnToCheckValue;
    private List<?> valuesIn = new ArrayList<>();
}
