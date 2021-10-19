package com.mercadolibre.flowbacklogchecker;

import dev.miku.r2dbc.mysql.MySqlConnectionConfiguration;
import dev.miku.r2dbc.mysql.MySqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.r2dbc.connection.init.CompositeDatabasePopulator;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;


@Configuration
@RequiredArgsConstructor
public class R2dbcConfig extends AbstractR2dbcConfiguration {
	private final static Logger log = LoggerFactory.getLogger(R2dbcConfig.class);

	private final Environment environment;

	@Bean("connectionFactory")
	public ConnectionFactory connectionFactory() {
		log.info("connectionFactory bean's factory method begin");
		var cf = MySqlConnectionFactory.from(
				MySqlConnectionConfiguration.builder()
						.host("proxysql.slave.meliseginf.com")
						.username(environment.getProperty("db.user"))
						.port(6612)
						.password(environment.getProperty("db.password"))
						.database("backlogprd")
						.connectTimeout(Duration.ofSeconds(9))
						.useServerPrepareStatement()
						.autodetectExtensions(true)
						.build()
		);
		log.info("connectionFactory bean's factory method end");
		return cf;
	}
}