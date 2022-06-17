package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


@Document(collection = "Users")
public class User {
   
   @Id
   private ObjectId id;
   
   private List<ObjectId> databaseId;

   private String email;
   private String bCryptPassword;
   
   private String uuid;
   
   private UserType type;

   private String name;
   private String jobDescription;

   private List<ObjectId> validatorList;
   private boolean isPublic;
   
   public User() {  }
   
   public User(String email, String bCryptPassword, String uuid, String name, UserType type) {
       this.email = email;
       this.bCryptPassword = bCryptPassword;
       this.uuid = uuid;
       this.type = type;
       
       this.databaseId = new ArrayList<>();
       this.name = name;
       this.jobDescription = "Validator";
       this.validatorList = new ArrayList<>();
       this.isPublic = false;
   }

   public ObjectId getId() {
	   return id;
   }
   
   public String getEmail() {
       return email;
   }
   
   public String getBCryptPassword() {
       return bCryptPassword;
   }
   
   public String getUuid() {
	   return uuid;
   }

   public String getName() {
    return name;
   }

   public void setName(String name) {
        this.name = name;
   }

   public String getJobDescription() {
    return jobDescription;
   }

   public void setJobDescription(String jobDescription) {
        this.jobDescription = jobDescription;
   }

   public boolean getIsPublic() {
    return isPublic;
   }

   public void setIsPublic(boolean isPublic) {
        this.isPublic = isPublic;
   }

   public List<ObjectId> getValidatorList() {
       return validatorList;
   }

   public void setValidatorList(List<ObjectId> validatorList) {
       this.validatorList = validatorList;
   }

   public void addValidator(ObjectId id) {
       this.validatorList.add(id);
   }
   
   @Override
   public String toString() {
       return String.format("User{id='%s', email='%s', bCryptPassword=%s}\n",
               id, email, bCryptPassword);
   }

	public UserType getType() {
		return type;
	}
	
	public void setType(UserType type) {
		this.type = type;
	}

	public List<ObjectId> getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(List<ObjectId> databaseId) {
		this.databaseId = databaseId;
	}

	public void addDatabaseId(ObjectId databaseId) {
		this.databaseId.add(databaseId);
	}

    public void setEmail(String email) {
        this.email = email;
    }

    public String getbCryptPassword() {
        return bCryptPassword;
    }

    public void setbCryptPassword(String bCryptPassword) {
        this.bCryptPassword = bCryptPassword;
    }

    public boolean isPublic() {
        return isPublic;
    }

    public void setPublic(boolean aPublic) {
        isPublic = aPublic;
    }
}