package ac.software.semantic.payload.request;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.annotation.JsonProperty;

import ac.software.semantic.model.RemoteTripleStore;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.VocabularyEntityDescriptor;
import ac.software.semantic.model.constants.type.DatasetCategory;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.payload.response.TemplateResponse;

public class DatasetUpdateRequest implements UpdateRequest {

	private String name;
   	private List<String> typeUri;
    
   	private String identifier;
   	
   	@JsonProperty("public")
    private boolean publik;
    
   	private DatasetCategory category;
    private DatasetScope scope;
   	private DatasetType type;

    private List<ResourceOption<ObjectId>> links;

	private String asProperty;
   	
	private RemoteTripleStore remoteTripleStore;
	
	private List<VocabularyEntityDescriptor> entityDescriptors;

	private TemplateResponse template;
	
	private boolean multiGroup;
	private List<Integer> publicGroups;
	
	public DatasetUpdateRequest() {
	   this.links = new ArrayList<>();
	}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
	public DatasetType getType() {
		return type;
	}

	public void setType(DatasetType type) {
		this.type = type;
	}
	
	public List<String> getTypeUri() {
		return typeUri;
	}

	public void setTypeUri(List<String> typeUri) {
		this.typeUri = typeUri;
	}

	public List<ResourceOption<ObjectId>> getLinks() {
		return links;
	}

	public void setLinks(List<ResourceOption<ObjectId>> links) {
		this.links = links;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public boolean isPublik() {
		return publik;
	}

	public void setPublik(boolean publik) {
		this.publik = publik;
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}

	public DatasetScope getScope() {
		return scope;
	}

	public void setScope(DatasetScope scope) {
		this.scope = scope;
	}


	public RemoteTripleStore getRemoteTripleStore() {
		return remoteTripleStore;
	}

	public void setRemoteTripleStore(RemoteTripleStore remoteTripleStore) {
		this.remoteTripleStore = remoteTripleStore;
	}

	public DatasetCategory getCategory() {
		return category;
	}

	public void setCategory(DatasetCategory category) {
		this.category = category;
	}

	public TemplateResponse getTemplate() {
		return template;
	}

	public void setTemplate(TemplateResponse template) {
		this.template = template;
	}

	public List<VocabularyEntityDescriptor> getEntityDescriptors() {
		return entityDescriptors;
	}

	public void setEntityDescriptors(List<VocabularyEntityDescriptor> entityDescriptors) {
		this.entityDescriptors = entityDescriptors;
	}

	public boolean isMultiGroup() {
		return multiGroup;
	}

	public void setMultiGroup(boolean multiGroup) {
		this.multiGroup = multiGroup;
	}

	public List<Integer> getPublicGroups() {
		return publicGroups;
	}

	public void setPublicGroups(List<Integer> publicGroups) {
		this.publicGroups = publicGroups;
	}


}
