package ac.software.semantic.config;

import org.locationtech.jts.geom.Point;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.module.SimpleModule;

import ac.software.util.PointSerializer;

@Configuration
class JacksonConfig {

    @Bean
    public com.fasterxml.jackson.databind.Module customSerializer() {
		SimpleModule module = new SimpleModule();
		module.addSerializer(Point.class, new PointSerializer());
		
		return module;
    }
}
