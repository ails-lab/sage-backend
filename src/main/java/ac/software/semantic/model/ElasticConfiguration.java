package ac.software.semantic.model;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.bson.types.ObjectId;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

@Document(collection = "ElasticConfigurations")
public class ElasticConfiguration implements ConfigurationObject {
	
	Logger logger = LoggerFactory.getLogger(TripleStoreConfiguration.class);
	
   @Id
   private ObjectId id;
   
   private ObjectId databaseId;
   
   private String name;
   
//	@Transient
	protected int order;
	
   private String indexIp;
   private int indexPort;
   private String protocol;
   
//   private String indexDataName;
//   private String indexVocabularyName;
   
	@Transient
	@JsonIgnore
	private String username;
	
	@Transient
	@JsonIgnore
	private String password;
	
	@Transient
	private String version;
	   
	@Transient
	@JsonIgnore
	ElasticsearchClient client;
	
   public ElasticConfiguration() { }

   	public ObjectId getId() {
   		return id;
   	}
	   
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

//	public String getIndexDataName() {
//		return indexDataName;
//	}
//
//	public void setIndexDataName(String indexDataName) {
//		this.indexDataName = indexDataName;
//	}
//
//	public String getIndexVocabularyName() {
//		return indexVocabularyName;
//	}
//
//	public void setIndexVocabularyName(String indexVocabularyName) {
//		this.indexVocabularyName = indexVocabularyName;
//	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public String getIndexIp() {
		return indexIp;
	}

	public void setIndexIp(String indexIp) {
		this.indexIp = indexIp;
	}

	public int getIndexPort() {
		return indexPort;
	}

	public void setIndexPort(int indexPort) {
		this.indexPort = indexPort;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

    public void connect() throws Exception {
		logger.info("Connecting to Elasticsearch " + getIndexIp() + ":" + getIndexPort());
		
		RestClientBuilder builder = RestClient.builder(new HttpHost(getIndexIp(), getIndexPort(), getProtocol()));
				
		if (username != null) {
    		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
			
    		builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
	            @Override
	            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//	                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
	            	
					try {
		        		SSLContextBuilder builder = new SSLContextBuilder();
		        		builder.loadTrustMaterial(null, new TrustAllStrategy());
		        		SSLContext sslContext = builder.build();

		        		return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLContext(sslContext).setHostnameVerifier(SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
					} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
						e.printStackTrace();
						
						return null;
					}
	            }
	        });
		}

		RestClient restClient = builder.build();
		
		ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

		client = new ElasticsearchClient(transport);
    }
    
    public ElasticsearchClient getClient() throws Exception {
    	if (client == null) {
    		connect();
    	}
    	
    	return client;
    }

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getVersion() {
		return version;
	}
//
//	public void setVersion(String version) {
//		this.version = version;
//	}

	public boolean test() {
		try {
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
			      .setSSLContext(sslContext)
			      .setSSLHostnameVerifier(new NoopHostnameVerifier());

			if (username != null) {
				CredentialsProvider provider = new BasicCredentialsProvider();
				UsernamePasswordCredentials cr = new UsernamePasswordCredentials(username, password);
				provider.setCredentials(AuthScope.ANY, cr);
					
				clientBuilder = clientBuilder.setDefaultCredentialsProvider(provider);
			}
					 
			HttpClient client = clientBuilder.build();
				
			HttpGet req = new HttpGet(protocol + "://" + indexIp + ":" + indexPort);
			HttpResponse response = client.execute(req);
			
			int responseCode = response.getStatusLine().getStatusCode();
			
			if (responseCode == 200) {
				ObjectMapper objectMapper = new ObjectMapper();
	
				Map res = objectMapper.readValue(response.getEntity().getContent(), Map.class);
				version = ((Map)res.get("version")).get("number").toString();
				
				return true;
			} else {
				return false;
			}

		} catch (Exception e) {
			e.printStackTrace();
			
			return false;
		}
//
		
		
	}
}
