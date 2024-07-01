package ac.software.semantic.payload.response.modifier;

import ac.software.semantic.payload.response.ResponseFieldType;

public class ProjectResponseModifier implements ResponseModifier {

	private ResponseFieldType joinedUsers;
	
	public ProjectResponseModifier() {
		joinedUsers = ResponseFieldType.IGNORE;
	}
	
	public ResponseFieldType getJoinedUsers() {
		return joinedUsers;
	}

	public void setJoinedUsers(ResponseFieldType users) {
		this.joinedUsers = users;
	}

}
