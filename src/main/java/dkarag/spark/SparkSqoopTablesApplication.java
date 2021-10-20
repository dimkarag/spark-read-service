package dkarag.spark;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkSqoopTablesApplication {

    @Value("${spring.profiles.active}")
    private String environment;

    public static void main(String[] args) {
        SpringApplication.run(SparkSqoopTablesApplication.class, args);
    }

}
