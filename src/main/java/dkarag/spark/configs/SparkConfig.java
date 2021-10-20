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
        masterHost = this.environment.getProperty("dkarag.spark.host");
        applicationName = this.environment.getProperty("dkarag.spark.appName") + '-' + jobName;
        executorsCores = this.environment.getProperty("dkarag.spark.executorCores");
        coresPerSparkSession = this.environment.getProperty("dkarag.spark.coresPerSparkSession");
        memoryPerExecutor = this.environment.getProperty("dkarag.spark.memoryPerExecutor");
        parquetFolderPath = this.environment.getProperty("dkarag.spark.parquetFolderPath");
    }
}
