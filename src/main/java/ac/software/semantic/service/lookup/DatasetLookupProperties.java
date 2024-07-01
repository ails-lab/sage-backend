package ac.software.semantic.service.lookup;

import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetType;

public class DatasetLookupProperties implements LookupProperties {
	
	private DatasetType datasetType;
	private List<DatasetScope> datasetScope;
	
	private Boolean onlyPublished;
	private Boolean publik;
	
	private ObjectId userIdNot;
	
	private List<String> sortByFields;
	
//	private ObjectId projectId;
	
	public List<DatasetScope> getDatasetScope() {
		return datasetScope;
	}
	
	public void setDatasetScope(List<DatasetScope> scope) {
		this.datasetScope = scope;
	}
	
	public DatasetType getDatasetType() {
		return datasetType;
	}
	
	public void setDatasetType(DatasetType type) {
		this.datasetType = type;
	}

	public Boolean getOnlyPublished() {
		return onlyPublished;
	}

	public void setOnlyPublished(Boolean onlyPublished) {
		this.onlyPublished = onlyPublished;
	}

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

	public List<String> getSortByFields() {
		return sortByFields;
	}

	public void setSortByFields(List<String> sortByFields) {
		this.sortByFields = sortByFields;
	}

//	public ObjectId getProjectId() {
//		return projectId;
//	}
//
//	public void setProjectId(ObjectId projectId) {
//		this.projectId = projectId;
//	}

}
