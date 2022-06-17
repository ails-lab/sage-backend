package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "Databases")
public class Database {
   @Id
   private ObjectId id;

   private String name;
   
   private String label;
   
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



}
