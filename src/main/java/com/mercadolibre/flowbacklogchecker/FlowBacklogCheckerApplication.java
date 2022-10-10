package com.mercadolibre.flowbacklogchecker;

import com.mercadolibre.flowbacklogchecker.consolidation.Boot;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlowBacklogCheckerApplication {

	public static void main(String[] args) {
		var boot = new Boot();
		boot.start(Long.parseLong(args[0]));
	}

}
