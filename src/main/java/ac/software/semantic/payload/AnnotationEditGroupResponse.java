package ac.software.semantic.payload;

import java.util.Date;
import java.util.List;

import org.springframework.data.annotation.Id;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DatasetState;
import ac.software.semantic.model.MappingState;

public class AnnotationEditGroupResponse {

	@Id
	private String id;

	private String uuid;
	
	private String datasetUuid;
	   
	private List<String> onProperty;
	private String asProperty;
	   
//	private MappingState executeState;
//	private DatasetState publishState;

//	@JsonInclude(JsonInclude.Include.NON_NULL)
//	private Date executeStartedAt;
//
//	@JsonInclude(JsonInclude.Include.NON_NULL)
//	private Date executeCompletedAt;
//
//	@JsonInclude(JsonInclude.Include.NON_NULL)
//	private Date publishStartedAt;
//
//	@JsonInclude(JsonInclude.Include.NON_NULL)
//	private Date publishCompletedAt;

	private int count;

	private List<PagedAnnotationValidationResponse> pagedAnnotationValidations;
	private List<FilterAnnotationValidationResponse> filterAnnotationValidations;

	public AnnotationEditGroupResponse() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

//	public MappingState getExecuteState() {
//		return executeState;
//	}
//
//	public void setExecuteState(MappingState executeState) {
//		this.executeState = executeState;
//	}
//
//	public DatasetState getPublishState() {
//		return publishState;
//	}
//
//	public void setPublishState(DatasetState publishState) {
//		this.publishState = publishState;
//	}

//	public Date getExecuteStartedAt() {
//		return executeStartedAt;
//	}
//
//	public void setExecuteStartedAt(Date executeStartedAt) {
//		this.executeStartedAt = executeStartedAt;
//	}
//
//	public Date getExecuteCompletedAt() {
//		return executeCompletedAt;
//	}
//
//	public void setExecuteCompletedAt(Date executeCompletedAt) {
//		this.executeCompletedAt = executeCompletedAt;
//	}
//
//	public Date getPublishStartedAt() {
//		return publishStartedAt;
//	}
//
//	public void setPublishStartedAt(Date publishStartedAt) {
//		this.publishStartedAt = publishStartedAt;
//	}
//
//	public Date getPublishCompletedAt() {
//		return publishCompletedAt;
//	}
//
//	public void setPublishCompletedAt(Date publishCompletedAt) {
//		this.publishCompletedAt = publishCompletedAt;
//	}

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

	public List<String> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<String> onProperty) {
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
}
