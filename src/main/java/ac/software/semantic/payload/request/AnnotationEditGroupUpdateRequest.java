package ac.software.semantic.payload.request;

import java.util.List;

public class AnnotationEditGroupUpdateRequest implements UpdateRequest {

	private boolean autoexportable;
	
	private String asProperty;
	private List<String> onProperty;
	
	private String onClass;
	private List<String> keys;
	
	private String tag;
//	private String sparqlClause;

	public AnnotationEditGroupUpdateRequest() {
		
	}

	public boolean isAutoexportable() {
		return autoexportable;
	}

	public void setAutoexportable(boolean autoexportable) {
		this.autoexportable = autoexportable;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public List<String> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<String> onProperty) {
		this.onProperty = onProperty;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

	public List<String> getKeys() {
		return keys;
	}

	public void setKeys(List<String> keys) {
		this.keys = keys;
	}

//	public String getSparqlClause() {
//		return sparqlClause;
//	}
//
//	public void setSparqlClause(String sparqlClause) {
//		this.sparqlClause = sparqlClause;
//	}
	
	
}
