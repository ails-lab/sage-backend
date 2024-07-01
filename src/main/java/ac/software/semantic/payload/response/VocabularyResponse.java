package ac.software.semantic.payload.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.VocabularyEntityDescriptor;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class VocabularyResponse {

   private String id;
	
   private String name;
   
   private List<VocabularyEntityDescriptor> uriDescriptors;
   
   private String specification;

   private List<String> classes;
   private List<String> properties;
   
   public VocabularyResponse() {
   }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSpecification() {
		return specification;
	}

	public void setSpecification(String specification) {
		this.specification = specification;
	}

	public List<String> getClasses() {
		return classes;
	}

	public void setClasses(List<String> classes) {
		this.classes = classes;
	}

	public List<String> getProperties() {
		return properties;
	}

	public void setProperties(List<String> properties) {
		this.properties = properties;
	}

	public List<VocabularyEntityDescriptor> getUriDescriptors() {
		return uriDescriptors;
	}

	public void setUriDescriptors(List<VocabularyEntityDescriptor> uriDescriptors) {
		this.uriDescriptors = uriDescriptors;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
