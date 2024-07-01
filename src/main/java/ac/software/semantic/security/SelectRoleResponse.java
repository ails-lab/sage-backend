package ac.software.semantic.security;

import java.util.List;

import ac.software.semantic.model.constants.type.UserRoleType;

public class SelectRoleResponse {
    private List<UserRoleType> roles;

    public SelectRoleResponse(List<UserRoleType> roles) {
        this.roles = roles;
    }

	public List<UserRoleType> getRoles() {
		return roles;
	}

	public void setRoles(List<UserRoleType> roles) {
		this.roles = roles;
	}
}

