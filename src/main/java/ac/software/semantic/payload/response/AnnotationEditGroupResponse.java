package ac.software.semantic.payload.response;

import java.util.List;

import org.springframework.data.annotation.Id;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.PathElement;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnnotationEditGroupResponse implements Response {

	@Id
	private String id;

	private String uuid;
	
	private String datasetUuid;
	   
	private List<PathElement> onProperty;
	private String onClass;
	
	private String asProperty;
	   
	private int count;

	private List<PagedAnnotationValidationResponse> pagedAnnotationValidations;
	private List<FilterAnnotationValidationResponse> filterAnnotationValidations;

	private boolean autoexportable;
	private boolean published;
	
	private String tag;
//	private String sparqlClause;
	
	private List<String> keys;
	   
	public AnnotationEditGroupResponse() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public List<PathElement> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<PathElement> onProperty) {
		this.onProperty = onProperty;
	}

	public String getAsProperty() {
		return asProperty;
	}
	
	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public List<PagedAnnotationValidationResponse> getPagedAnnotationValidations() {
		return pagedAnnotationValidations;
	}

	public void setPagedAnnotationValidations(List<PagedAnnotationValidationResponse> pagedAnnotationValidations) {
		this.pagedAnnotationValidations = pagedAnnotationValidations;
	}

	public List<FilterAnnotationValidationResponse> getFilterAnnotationValidations() {
		return filterAnnotationValidations;
	}

	public void setFilterAnnotationValidations(List<FilterAnnotationValidationResponse> filterAnnotationValidations) {
		this.filterAnnotationValidations = filterAnnotationValidations;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public boolean isAutoexportable() {
		return autoexportable;
	}

	public void setAutoexportable(boolean autoexportable) {
		this.autoexportable = autoexportable;
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

	public boolean isPublished() {
		return published;
	}

	public void setPublished(boolean published) {
		this.published = published;
	}

//	public String getSparqlClause() {
//		return sparqlClause;
//	}
//
//	public void setSparqlClause(String sparqlClause) {
//		this.sparqlClause = sparqlClause;
//	}
}
