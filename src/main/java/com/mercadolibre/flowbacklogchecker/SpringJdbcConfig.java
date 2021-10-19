package com.mercadolibre.flowbacklogchecker;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@ConfigurationProperties(prefix = "spring.datasource")
@Configuration
@Data
public class SpringJdbcConfig {

	private String url;

	private String username;

	private String password;

	private String driverClassName;

	@Bean("mysqlDataSource")
	public DataSource mysqlDataSource() {
		return DataSourceBuilder.create()
				.username(username)
				.password(password)
				.driverClassName(driverClassName)
				.url(url)
				.build();
	}
}