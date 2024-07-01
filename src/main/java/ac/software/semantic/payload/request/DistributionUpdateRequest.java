package ac.software.semantic.payload.request;

import java.util.List;

import ac.software.semantic.model.constants.type.ExportVocabulary;
import ac.software.semantic.model.constants.type.SerializationType;

public class DistributionUpdateRequest implements UpdateRequest {

	private String name;
	
	private String identifier;
	
	private List<String> classes;
	private List<SerializationType> serializations;
	
	private ExportVocabulary serializationVocabulary;
	
	private String license;
	
	private String compress;
	
	public DistributionUpdateRequest() {		
	}

	public List<String> getClasses() {
		return classes;
	}

	public void setClasses(List<String> classes) {
		this.classes = classes;
	}

	public List<SerializationType> getSerializations() {
		return serializations;
	}

	public void setSerializations(List<SerializationType> serializations) {
		this.serializations = serializations;
	}

	public String getCompress() {
		return compress;
	}

	public void setCompress(String compress) {
		this.compress = compress;
	}

	public String getLicense() {
		return license;
	}

	public void setLicense(String license) {
		this.license = license;
	}

	public ExportVocabulary getSerializationVocabulary() {
		return serializationVocabulary;
	}

	public void setSerializationVocabulary(ExportVocabulary serializationVocabulary) {
		this.serializationVocabulary = serializationVocabulary;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	
	
}
