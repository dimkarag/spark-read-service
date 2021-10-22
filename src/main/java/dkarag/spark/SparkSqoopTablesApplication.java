package dkarag.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
public class SparkSqoopTablesApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkSqoopTablesApplication.class, args);
    }

}
