package ac.software.semantic.payload.request;


import java.util.List;

import javax.validation.constraints.NotBlank;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.UserRoleType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class InviteUserRequest {
    @NotBlank
    private String email;

    private List<ObjectId> projectId;
    private List<UserRoleType> role;
    
    public InviteUserRequest() {
    	
    }

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public List<ObjectId> getProjectId() {
		return projectId;
	}

	public void setProjectId(List<ObjectId> projectId) {
		this.projectId = projectId;
	}

	public List<UserRoleType> getRole() {
		return role;
	}

	public void setRole(List<UserRoleType> role) {
		this.role = role;
	}
}

