package ac.software.semantic;

import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

@Component
public class CustomContainer implements
  WebServerFactoryCustomizer<TomcatServletWebServerFactory> {
 
    @Override
    public void customize(TomcatServletWebServerFactory factory) {
         factory.addConnectorCustomizers(
             (connector) -> {
                 connector.setMaxPostSize(20000000); // 10 MB
             }
         );
    }
}
