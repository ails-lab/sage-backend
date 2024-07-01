package ac.software.semantic.model;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.constants.type.UserRoleType;

public class TokenDetails {

	private ObjectId userId; // target user
	private String email; // target email
	
	private List<UserRoleType> role; // for sign up
	private List<ObjectId> project;  // for sign up
	
	public TokenDetails() {
		
	}

	public List<UserRoleType> getRole() {
		return role;
	}

	public void setRole(List<UserRoleType> role) {
		this.role = role;
	}

	public List<ObjectId> getProject() {
		return project;
	}

	public void setProject(List<ObjectId> project) {
		this.project = project;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	
}
