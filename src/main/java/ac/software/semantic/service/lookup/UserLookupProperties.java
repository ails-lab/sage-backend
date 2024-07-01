package ac.software.semantic.service.lookup;

import ac.software.semantic.model.constants.type.UserRoleType;

public class UserLookupProperties implements LookupProperties {
	
	private UserRoleType userRoleType;

	public UserRoleType getUserRoleType() {
		return userRoleType;
	}

	public void setUserRoleType(UserRoleType userRoleType) {
		this.userRoleType = userRoleType;
	}


}
