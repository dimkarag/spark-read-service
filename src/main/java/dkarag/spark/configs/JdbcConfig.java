package dkarag.spark.configs;

import lombok.Getter;
import lombok.Setter;
import org.springframework.core.env.Environment;

@Getter
@Setter
public class JdbcConfig {
    private final Environment environment;
    private String url;
    private String username;
    private String password;
    public JdbcConfig(Environment environment) {
        this.environment = environment;
        url = this.environment.getProperty("database.jdbc.url");
        username = this.environment.getProperty("database.jdbc.username");
        password = this.environment.getProperty("database.jdbc.password");
    }
}
