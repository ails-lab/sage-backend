package ac.software.semantic.model;

import org.bson.types.ObjectId;

public class PasswordResetTokenDetails {

	private ObjectId userId;
	
	public PasswordResetTokenDetails() {
		
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

}
