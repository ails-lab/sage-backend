package ac.software.semantic.service;

import java.io.StringWriter;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.LodViewConfiguration;
import ac.software.semantic.payload.LodViewRequest;
import ac.software.semantic.payload.PrefixEndpoint;
import edu.ntua.isci.ac.d2rml.model.informationsource.StandardCredentials;

@Service
public class LodViewService {

	Logger logger = LoggerFactory.getLogger(LodViewService.class);
	
	@Autowired
	private NamesService namesService;
	
    @Autowired
    @Qualifier("database")
    private Database database;
    
    @Autowired
    @Qualifier("lodview-configuration")
    private LodViewConfiguration lodViewConfiguration;

	public void updateLodView() {
		if (lodViewConfiguration != null) {
			Collection<PrefixEndpoint> prefixes = namesService.getPrefixEndpoints();
			
//			System.out.println(database.getName());
//			for (PrefixEndpoint p : prefixes) {
//				System.out.println(p.getPrefix() + " " + p.getEndpoint());
//			}
			
			try {
				HttpClient httpClient = getClient();
				
				ObjectMapper mapper = new ObjectMapper();
				
				HttpPost request = new HttpPost(lodViewConfiguration.getBaseUrl() + "prefix/reset?api-key=" + lodViewConfiguration.getApiKey());
				request.setHeader("Content-type", "application/json");
				
				StringWriter sw = new StringWriter();
				
				mapper.writeValue(sw, new LodViewRequest(database.getName(), prefixes, lodViewConfiguration.getHosts()));
				
				request.setEntity(new StringEntity(sw.toString()));
				
				HttpResponse response = httpClient.execute(request);

				logger.info("Updated LodView " + lodViewConfiguration.getName() + " : " + response.getStatusLine().getStatusCode());
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	private HttpClient getClient() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		SSLContext sslContext = new SSLContextBuilder()
			      .loadTrustMaterial(null, new TrustStrategy() {
					@Override
					public boolean isTrusted(X509Certificate[] certificate, String authType)
							throws CertificateException {
						return true;
					}
				}).build();
	
		HttpClientBuilder clientBuilder = HttpClients.custom();
		
		clientBuilder = clientBuilder
		//.setRedirectStrategy(new LaxRedirectStrategy())
	    .setSSLContext(sslContext)
	    .setSSLHostnameVerifier(new NoopHostnameVerifier());
	
		return clientBuilder.build();
	}
}
