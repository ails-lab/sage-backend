package ac.software.semantic.payload.request;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.constants.type.MappingType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MappingUpdateRequest implements MultipartFileUpdateRequest, InGroupRequest {

	private String name; 
	private MappingType type;
//	private String d2rml;
	
    private String description;
	
	private String identifier;
	private List<String> groupIdentifiers;
	
	@JsonIgnore
	private MultipartFile file;
	
	private String templateId;
	private String d2rmlId;
	private boolean d2rmlIdBound;
	
	private List<DataServiceParameter> parameters;
	private List<String> dependencies;
	
	private List<String> shaclId;
	
	private Boolean active;
	private int group;
	
	public MappingUpdateRequest() { }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

//	public String getD2rml() {
//		return d2rml;
//	}
//
//	public void setD2rml(String d2rml) {
//		this.d2rml = d2rml;
//	}

	public List<DataServiceParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<DataServiceParameter> parameters) {
		this.parameters = parameters;
	}

	public String getTemplateId() {
		return templateId;
	}

	public void setTemplateId(String templateId) {
		this.templateId = templateId;
	}

	public List<String> getShaclId() {
		return shaclId;
	}

	public void setShaclId(List<String> shaclId) {
		this.shaclId = shaclId;
	}
	
	public void setShaclIdFromObjectIds(List<ObjectId> shaclId) {
		if (shaclId == null) {
			this.shaclId = null;
		} else {
			this.shaclId = new ArrayList<>();
			for (ObjectId id : shaclId) {
				this.shaclId.add(id.toString());
			}
		}
	}

	@Override
	public MultipartFile getFile() {
		return file;
	}

	@Override
	public void setFile(MultipartFile file) {
		this.file = file;
	}

	public MappingType getType() {
		return type;
	}

	public void setType(MappingType type) {
		this.type = type;
	}

	public Boolean isActive() {
		return active;
	}

	public void setActive(Boolean active) {
		this.active = active;
	}

	public String getD2rmlId() {
		return d2rmlId;
	}

	public void setD2rmlId(String d2rmlId) {
		this.d2rmlId = d2rmlId;
	}

	public List<String> getDependencies() {
		return dependencies;
	}

	public void setDependencies(List<String> dependencies) {
		this.dependencies = dependencies;
	}

	public boolean isD2rmlIdBound() {
		return d2rmlIdBound;
	}

	public void setD2rmlIdBound(boolean d2rmlIdBound) {
		this.d2rmlIdBound = d2rmlIdBound;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public List<String> getGroupIdentifiers() {
		return groupIdentifiers;
	}

	public void setGroupIdentifiers(List<String> groupIdentifiers) {
		this.groupIdentifiers = groupIdentifiers;
	}

	@Override
	public int getGroup() {
		return group;
	}

	@Override
	public void setGroup(int group) {
		this.group = group;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
}