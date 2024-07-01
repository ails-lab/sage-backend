package ac.software.semantic.payload.response;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.PreprocessInstruction;
import ac.software.semantic.model.SchemaSelector;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.payload.PrefixizedUri;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnnotatorDocumentResponse implements Response, ExecutePublishResponse, MultiUserResponse {
   
   private String id;
   private String uuid;

   private List<PathElement> onProperty;
   private String asProperty;
   private String annotator;
   private String annotatorId;
   private String annotatorName; // name for annotatorId
   
   private String variant;
   
//   private String thesaurus;
   private String thesaurusId;
   private String thesaurusName;
   
   private ResponseTaskObject executeState;
   private ResponseTaskObject publishState;
   
   private List<DataServiceParameterValue> parameters;
   
   private List<PreprocessInstruction> preprocess;
   
   private String name;
   private String identifier;
//   private AnnotationEditGroupResponse editGroup;
   
   private Boolean publishedFromCurrentFileSystem;
   private Boolean newExecution;
   
   private Date createdAt;
   private Date updatedAt;
   
   private String onClass;
	private ClassIndexElementResponse element;
	private List<IndexKeyMetadata> keysMetadata;
	
	private SchemaSelector control;
	
	private List<String> bodyProperties;
   
   @JsonInclude(JsonInclude.Include.NON_NULL)
   private PrefixizedUri defaultTarget;
   
   private List<String> tags;
   
   private boolean ownedByUser;
   
   private int order;
   private int group;

   public AnnotatorDocumentResponse() {
   }
   
   public String getId() {
       return id;
   }
   
	public void setId(String id) {
		this.id = id;
	}

	public String getAnnotator() {
		return annotator;
	}

	public void setAnnotator(String annotator) {
		this.annotator = annotator;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public List<PathElement> getOnProperty() {
		return onProperty;
	}

	public void setOnProperty(List<PathElement> onProperty) {
		this.onProperty = onProperty;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

//	public String getThesaurus() {
//		return thesaurus;
//	}
//
//	public void setThesaurus(String thesaurus) {
//		this.thesaurus = thesaurus;
//	}

//	public AnnotationEditGroupResponse getEditGroup() {
//		return editGroup;
//	}
//
//	public void setEditGroup(AnnotationEditGroupResponse editGroup) {
//		this.editGroup = editGroup;
//	}

	public List<DataServiceParameterValue> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameterValue> parameters) {
		this.parameters = parameters;
	}

	public List<PreprocessInstruction> getPreprocess() {
		return preprocess;
	}

	public void setPreprocess(List<PreprocessInstruction> preprocess) {
		this.preprocess = preprocess;
	}

	public String getVariant() {
		return variant;
	}

	public void setVariant(String variant) {
		this.variant = variant;
	}

	public PrefixizedUri getDefaultTarget() {
		return defaultTarget;
	}

	public void setDefaultTarget(PrefixizedUri defaultTarget) {
		this.defaultTarget = defaultTarget;
	}

	public Boolean isPublishedFromCurrentFileSystem() {
		return publishedFromCurrentFileSystem;
	}

	public void setPublishedFromCurrentFileSystem(Boolean publishedFromCurrentFileSystem) {
		this.publishedFromCurrentFileSystem = publishedFromCurrentFileSystem;
	}

	public Boolean isNewExecution() {
		return newExecution;
	}

	public void setNewExecution(Boolean newExecution) {
		this.newExecution = newExecution;
	}


	public ResponseTaskObject getExecuteState() {
		return executeState;
	}

	public void setExecuteState(ResponseTaskObject executeState) {
		this.executeState = executeState;
	}

	public ResponseTaskObject getPublishState() {
		return publishState;
	}

	public void setPublishState(ResponseTaskObject publishState) {
		this.publishState = publishState;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOnClass() {
		return onClass;
	}

	public void setOnClass(String onClass) {
		this.onClass = onClass;
	}

	public String getAnnotatorId() {
		return annotatorId;
	}

	public void setAnnotatorId(String annotatorId) {
		this.annotatorId = annotatorId;
	}

	public List<IndexKeyMetadata> getKeysMetadata() {
		return keysMetadata;
	}

	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
		this.keysMetadata = keysMetadata;
	}

	public ClassIndexElementResponse getElement() {
		return element;
	}

	public void setElement(ClassIndexElementResponse element) {
		this.element = element;
	}

	public String getAnnotatorName() {
		return annotatorName;
	}

	public void setAnnotatorName(String annotatorName) {
		this.annotatorName = annotatorName;
	}

	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}

	public String getThesaurusId() {
		return thesaurusId;
	}

	public void setThesaurusId(String thesaurusId) {
		this.thesaurusId = thesaurusId;
	}

	public String getThesaurusName() {
		return thesaurusName;
	}

	public void setThesaurusName(String thesaurusName) {
		this.thesaurusName = thesaurusName;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public List<String> getBodyProperties() {
		return bodyProperties;
	}

	public void setBodyProperties(List<String> bodyProperties) {
		this.bodyProperties = bodyProperties;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public int getGroup() {
		return group;
	}

	public void setGroup(int group) {
		this.group = group;
	}

	public SchemaSelector getControl() {
		return control;
	}

	public void setControl(SchemaSelector control) {
		this.control = control;
	}
}