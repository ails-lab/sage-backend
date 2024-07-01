package ac.software.semantic.model;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.UserRoleType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserRoleDefault {

	private UserRoleType role;
	
	private ObjectId defaultProjectId;

	public UserRoleDefault() {
		
	}
	
	public UserRoleType getRole() {
		return role;
	}

	public void setRole(UserRoleType role) {
		this.role = role;
	}

	public ObjectId getDefaultProjectId() {
		return defaultProjectId;
	}

	public void setDefaultProjectId(ObjectId defaultProjectId) {
		this.defaultProjectId = defaultProjectId;
	}
	
}
