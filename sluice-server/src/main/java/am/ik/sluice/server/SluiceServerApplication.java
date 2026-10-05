package am.ik.sluice.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SluiceServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SluiceServerApplication.class, args);
	}

}
