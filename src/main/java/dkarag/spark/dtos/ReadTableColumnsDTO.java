package dkarag.spark.dtos;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class ReadTableColumnsDTO {
    private String tableName;
    private List<String> columnsToSelect = new ArrayList<>();
}
