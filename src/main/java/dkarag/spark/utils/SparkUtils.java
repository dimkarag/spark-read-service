package dkarag.spark.utils;


import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.springframework.core.env.Environment;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import dkarag.spark.configs.JdbcConfig;
import dkarag.spark.configs.SparkConfig;

@RequiredArgsConstructor
public class SparkUtils {
    private SparkSession.Builder sparkSessionBuilder;
    private SparkSession sparkSession;
    private SparkConfig sparkConfig;
    private JdbcConfig jdbcConfig;
    private final JdbcUtils jdbcUtils;
    private final Environment environment;

    public SparkSession createSparkSession(String jobName) {
        initSparkSession(jobName);
        sparkSession = sparkSessionBuilder.getOrCreate();
        return sparkSession;
    }

    public void closeSpark() {
        if (sparkSession != null) {
            SparkSession.clearActiveSession();
            sparkSession.close();
        }
    }

    private void initSparkSession(String jobName){
        this.sparkConfig = new SparkConfig(environment, jobName);
        this.jdbcConfig = new JdbcConfig(environment);
        this.sparkSessionBuilder = SparkSession.builder().
                master(sparkConfig.getMasterHost()).
                appName(sparkConfig.getApplicationName()).
                config("dkarag.spark.cores.max", Integer.parseInt(sparkConfig.getCoresPerSparkSession())).
                config("dkarag.spark.executor.cores", sparkConfig.getExecutorsCores()).
                config("dkarag.spark.executor.memory", sparkConfig.getMemoryPerExecutor()).
                config("dkarag.spark.executor.extraJavaOptions", "-XX:+UseCompressedOops").
                config("dkarag.spark.sql.jsonGenerator.ignoreNullFields", false);
    }

    private void writeToParquet(String parquetFolderPath, Dataset<Row> jdbcDF, String tableName) {
        jdbcDF.write().parquet(parquetFolderPath + "/" + tableName);
    }

    public Dataset<Row> readFromParquet(String parquetFolderPath, String tableName){
        Dataset<Row> tableFromParquet = sparkSession.read().parquet(parquetFolderPath + "/" + tableName);
        return tableFromParquet;
    }

    private Dataset<Row> sqoopTableWithQuery(String query, String tableName, boolean writeToParquet) {
        Dataset<Row> data =  sparkSession.read().format("jdbc")
                .option("url", jdbcConfig.getUrl())
                .option("user", jdbcConfig.getUsername())
                .option("password", jdbcConfig.getPassword())
                .option("query", query)
                .load().repartition(Integer.parseInt(sparkConfig.getExecutorsCores()) * 2)
                .persist(StorageLevel.MEMORY_AND_DISK());

        if(writeToParquet) {
            this.writeToParquet(sparkConfig.getParquetFolderPath(), data, tableName);
        }
        return data;
    }

    public Dataset<Row> sqoopSpecificColumnsOfTable(String tableName, String[] columns, boolean writeToParquet) {
        String columnsToSelect = "*";
        if (columns!= null && columns.length > 0) {
            columnsToSelect = StringUtils.join(columns, ",");
        }
        String query = "SELECT " + columnsToSelect + " FROM " + tableName;

        return sqoopTableWithQuery(query, tableName, writeToParquet);
    }

    public Dataset<Row> sqoopTableInPartitions(String tableName, String partitionColumn, long valueToDivideCountOfRows, boolean writeToParquet) {
        Dataset<Row> data =  sparkSession.read()
                .option("url", jdbcConfig.getUrl())
                .option("user", jdbcConfig.getUsername())
                .option("password", jdbcConfig.getPassword())
                .option("partitionColumn", partitionColumn)
                .option("lowerBound", jdbcUtils.lowerBoundOfColumn(partitionColumn, tableName, jdbcConfig))
                .option("upperBound", jdbcUtils.upperBoundOfColumn(partitionColumn, tableName, jdbcConfig))
                .option("numPartitions", jdbcUtils.rowsOfTable(tableName, jdbcConfig) / valueToDivideCountOfRows)
                .load()
                .persist(StorageLevel.MEMORY_AND_DISK());

        if(writeToParquet) {
            this.writeToParquet(sparkConfig.getParquetFolderPath(), data, tableName);
        }
        return data;
    }



//    public Dataset<Row> sqoopTableColumnsWithColumnInValues(SparkRequirements sparkReqs, String tableName, String[] selectedColumns, String columnToCheckValues,  int... values) {
//        String columnsToSelect = "*";
//        if (selectedColumns!= null && selectedColumns.length > 0) {
//            columnsToSelect = StringUtils.join(selectedColumns, ",");
//        }
//        String valuesInString = StringUtils.join(ArrayUtils.toObject(values), ",");
//
//        if (valuesInString.isBlank()) {
//            valuesInString = "\'\'";
//        }
//        String query = "SELECT " + columnsToSelect + " FROM " + tableName + " WHERE " + columnToCheckValues + " IN (" + valuesInString + ")";
//
//        return getDataset(sparkReqs, query);
//    }


    public Dataset<Row> sqoopTableColumnsWithColumnInValues(String tableName, String columnToCheckValues, List<?> inValues, boolean writeToParquet, String ...selectedColumns) {
        String columnsToSelect = "*";
        if (selectedColumns!= null && selectedColumns.length > 0) {
            columnsToSelect = StringUtils.join(selectedColumns, ",");
        }

        List<String> validIds = inValues.stream()
                .map(s -> "'" + s + "'")
                .collect(Collectors.toList());
        String valuesInString = StringUtils.join(validIds, ",");

        if (valuesInString.isBlank()) {
            valuesInString = "\'\'";
        }

        String query = "SELECT " + columnsToSelect + " FROM " + tableName + " WHERE " + columnToCheckValues + " IN (" + valuesInString + ")";
        return sqoopTableWithQuery(query, tableName, writeToParquet);
    }

    public Dataset<Row> sqoopTableColumnsForDate(String tableName, String dateColumn, Timestamp date, boolean writeToParquet, String ...selectedColumns) {
        return this.sqoopTableColumnsForDateBetween(tableName, dateColumn, date, date, writeToParquet, selectedColumns);
    }

    public Dataset<Row> sqoopTableColumnsForDateRange(String tableName, String dateColumn, Timestamp startDate, Timestamp endDate, boolean writeToParquet, String ...selectedColumns) {
        return this.sqoopTableColumnsForDateBetween(tableName, dateColumn, startDate, endDate, writeToParquet, selectedColumns);
    }


    private Dataset<Row> sqoopTableColumnsForDateBetween(String tableName, String dateColumn, Timestamp startDate, Timestamp endDate, boolean writeToParquet, String ...selectedColumns) {
        String columnsToSelect = "*";
        if (selectedColumns.length > 0) {
            columnsToSelect = StringUtils.join(selectedColumns, ",");
        }

        String query = "SELECT "+ columnsToSelect +" FROM "+tableName+ " WHERE "+dateColumn+" >= '"+ startDate+"' AND "+dateColumn+" <= '"+endDate+"'";

        return sqoopTableWithQuery(query, tableName, writeToParquet);
    }

}
