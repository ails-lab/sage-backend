package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.config.ConfigurationObject;

@Document(collection = "ElasticConfigurations")
public class ElasticConfiguration implements ConfigurationObject {
   @Id
   private ObjectId id;
   
   private ObjectId databaseId;
   
   private String name;
   
//	@Transient
	protected int order;
	
   private String indexIp;
   private int indexPort;
   
   private String indexDataName;
   private String indexVocabularyName;
   
	@Transient
	@JsonIgnore
	private String username;
	
	@Transient
	@JsonIgnore
	private String password;
   
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

	public String getIndexDataName() {
		return indexDataName;
	}

	public void setIndexDataName(String indexDataName) {
		this.indexDataName = indexDataName;
	}

	public String getIndexVocabularyName() {
		return indexVocabularyName;
	}

	public void setIndexVocabularyName(String indexVocabularyName) {
		this.indexVocabularyName = indexVocabularyName;
	}

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

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}


}
