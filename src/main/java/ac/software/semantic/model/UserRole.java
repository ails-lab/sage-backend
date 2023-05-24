package ac.software.semantic.model;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.UserRoleType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "UserRoles")
public class UserRole {
   
   @Id
   private ObjectId id;
   
   private ObjectId userId;
   
   private ObjectId databaseId;

   private List<UserRoleType> role;
   
   private String name;
   
//   private List<ObjectId> validatorId;

   
   public UserRole() {  }
   
   public UserRole(ObjectId userId, ObjectId databaseId, List<UserRoleType> role) { 
	   this.userId = userId;
	   this.databaseId = databaseId;
	   this.role = role;
   }


	public ObjectId getUserId() {
		return userId;
	}
	
	
	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}
	
	
	public ObjectId getDatabaseId() {
		return databaseId;
	}
	
	
	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}
	
	
	public List<UserRoleType> getRole() {
		return role;
	}
	
	
	public void setRole(List<UserRoleType> role) {
		this.role = role;
	}
	
	
	public String getName() {
		return name;
	}
	
	
	public void setName(String name) {
		this.name = name;
	}


//	public List<ObjectId> getValidatorId() {
//		return validatorId;
//	}
//
//
//	public void setValidatorId(List<ObjectId> validatorId) {
//		this.validatorId = validatorId;
//	}
//	
//	public boolean addValidatorId(ObjectId id) {
//		if (validatorId == null) {
//			validatorId = new ArrayList<>();
//		}
//		
//		if (validatorId.contains(id)) {
//			return false;
//		} else {
//			validatorId.add(id);
//			return true;
//		}
//		
//	}
	
//	public boolean removeValidatorId(ObjectId id) {
//		boolean removed = false;
//		if (validatorId != null) {
//			removed = validatorId.remove(id);
//		} 
//
//		if (validatorId.size() == 0) {
//			validatorId = null;
//		}
//		
//		return removed;
//		
//	}
   

}