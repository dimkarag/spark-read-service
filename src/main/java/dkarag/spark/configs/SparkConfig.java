package dkarag.spark.configs;

import lombok.Getter;
import lombok.Setter;
import org.springframework.core.env.Environment;

@Getter
@Setter
public class SparkConfig {
    private final Environment environment;
    private String applicationName;
    private String masterHost;
    private String executorsCores;
    private String sparkDriverMemory;
    private String coresPerSparkSession;
    private String memoryPerExecutor;
    private String parquetFolderPath;
    public SparkConfig(Environment environment, String jobName) {
        this.environment = environment;
        masterHost = this.environment.getProperty("spark.host");
        applicationName = this.environment.getProperty("spark.appName") + '-' + jobName;
        executorsCores = this.environment.getProperty("spark.executorCores");
        coresPerSparkSession = this.environment.getProperty("spark.coresPerSparkSession");
        memoryPerExecutor = this.environment.getProperty("spark.memoryPerExecutor");
        parquetFolderPath = this.environment.getProperty("spark.parquetFolderPath");
    }
}
