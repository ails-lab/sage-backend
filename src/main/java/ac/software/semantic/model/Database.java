package ac.software.semantic.model;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.PathElementType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Databases")
public class Database {
   @Id
   private ObjectId id;

   private String name;
   
   private String label;
   
   private String resourcePrefix;
   
   private Compatibility compatibility;
   
   public Database() { }
   
   public Database(String name, String label) {
       this.name = name;
       this.label = label;
    		   
   }

   	public ObjectId getId() {
   		return id;
   	}
	   

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getResourcePrefix() {
		return resourcePrefix;
	}

	public void setResourcePrefix(String resourcePrefix) {
		this.resourcePrefix = resourcePrefix;
	}

	public Compatibility getCompatibility() {
		return compatibility;
	}

	public void setCompatibility(Compatibility compatibility) {
		this.compatibility = compatibility;
	}

	public class Compatibility {
		List<IRICompatibility> iris;
		
		public Compatibility() { }
		
		public List<IRICompatibility> getIris() {
			return iris;
		}

		public void setIris(List<IRICompatibility> iris) {
			this.iris = iris;
		}
	}



}
