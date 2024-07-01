package ac.software.semantic.payload.response.modifier;

import ac.software.semantic.payload.response.ResponseFieldType;

public class UserResponseModifier implements ResponseModifier {

//	private ResponseFieldType userRole;
	private ResponseFieldType datasetCount;
	private ResponseFieldType inOtherDatabases;
	private ResponseFieldType annotationEditAcceptCount;
	private ResponseFieldType annotationEditRejectCount;
	private ResponseFieldType annotationEditAddCount;

	private ResponseFieldType id;
	private ResponseFieldType roles;
	private ResponseFieldType email;
	
	public UserResponseModifier() {
		datasetCount = ResponseFieldType.IGNORE;
		inOtherDatabases = ResponseFieldType.IGNORE;
		annotationEditAcceptCount = ResponseFieldType.IGNORE;
		annotationEditRejectCount = ResponseFieldType.IGNORE;
		annotationEditAddCount = ResponseFieldType.IGNORE;
	}

	public ResponseFieldType getDatasetCount() {
		return datasetCount;
	}

	public void setDatasetCount(ResponseFieldType datasetCount) {
		this.datasetCount = datasetCount;
	}

	public ResponseFieldType getInOtherDatabases() {
		return inOtherDatabases;
	}

	public void setInOtherDatabases(ResponseFieldType inOtherDatabases) {
		this.inOtherDatabases = inOtherDatabases;
	}

	public ResponseFieldType getAnnotationEditAcceptCount() {
		return annotationEditAcceptCount;
	}

	public void setAnnotationEditAcceptCount(ResponseFieldType annotationEditAcceptCount) {
		this.annotationEditAcceptCount = annotationEditAcceptCount;
	}

	public ResponseFieldType getAnnotationEditRejectCount() {
		return annotationEditRejectCount;
	}

	public void setAnnotationEditRejectCount(ResponseFieldType annotationEditRejectCount) {
		this.annotationEditRejectCount = annotationEditRejectCount;
	}

	public ResponseFieldType getAnnotationEditAddCount() {
		return annotationEditAddCount;
	}

	public void setAnnotationEditAddCount(ResponseFieldType annotationEditAddCount) {
		this.annotationEditAddCount = annotationEditAddCount;
	}

	public ResponseFieldType getId() {
		return id;
	}

	public void setId(ResponseFieldType id) {
		this.id = id;
	}

	public ResponseFieldType getRoles() {
		return roles;
	}

	public void setRoles(ResponseFieldType roles) {
		this.roles = roles;
	}

	public ResponseFieldType getEmail() {
		return email;
	}

	public void setEmail(ResponseFieldType email) {
		this.email = email;
	}
}
