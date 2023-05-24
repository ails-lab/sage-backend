package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ListElement {
	
	private String datasetIdentifier;
	private String downloadUrl;
	
	private List<String> childrenDatasetIdentifiers;
	
	public ListElement() { }

	public String getDatasetIdentifier() {
		return datasetIdentifier;
	}

	public void setDatasetIdentifier(String identifier) {
		this.datasetIdentifier = identifier;
	}

	public String getDownloadUrl() {
		return downloadUrl;
	}

	public void setDownloadUrl(String url) {
		this.downloadUrl = url;
	}

	public List<String> getChildrenDatasetIdentifiers() {
		return childrenDatasetIdentifiers;
	}

	public void setChildrenDatasetIdentifiers(List<String> childrenDatasetIdentifiers) {
		this.childrenDatasetIdentifiers = childrenDatasetIdentifiers;
	}

	public void addChildrenDatasetIdentifier(String id) {
		if (childrenDatasetIdentifiers == null) {
			this.childrenDatasetIdentifiers = new ArrayList<>();
		}
		this.childrenDatasetIdentifiers.add(id);
	}
	
}
