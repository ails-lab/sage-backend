
package ac.software.semantic.service.lookup;

import org.bson.types.ObjectId;

public class ProjectLookupProperties implements LookupProperties {
	
	private ObjectId userIdNot;

	private ObjectId joinedUserId;
	
	private Boolean publik;
	
	public Boolean getPublik() {
		return publik;
	}

	public void setPublik(Boolean publik) {
		this.publik = publik;
	}

	public ObjectId getUserIdNot() {
		return userIdNot;
	}

	public void setUserIdNot(ObjectId userIdNot) {
		this.userIdNot = userIdNot;
	}

	public ObjectId getJoinedUserId() {
		return joinedUserId;
	}

	public void setJoinedUserId(ObjectId joinedUserId) {
		this.joinedUserId = joinedUserId;
	}

}
