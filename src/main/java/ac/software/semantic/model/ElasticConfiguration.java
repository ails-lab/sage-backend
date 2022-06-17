package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "ElasticConfigurations")
public class ElasticConfiguration {
   @Id
   private ObjectId id;
   
   private ObjectId databaseId;
   
   private String name;
   
   private String indexIp;
   
   private String indexDataName;
   private String indexVocabularyName;
   
   
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


}
