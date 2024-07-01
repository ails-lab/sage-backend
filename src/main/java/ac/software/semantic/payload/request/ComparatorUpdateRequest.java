package ac.software.semantic.payload.request;

import java.util.List;

import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.SchemaSelector;
import ac.software.semantic.model.constants.type.PrototypeType;
import ac.software.semantic.model.expr.Computation;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ComparatorUpdateRequest implements UpdateRequest {

	private String name;
	
	private String description;
	
//	private String onClass;
	
	private SchemaSelector structure;
//	private ClassIndexElement element;
//	private List<IndexKeyMetadata> keysMetadata;
	
	private Computation computation;
	
	private String schemaDatasetId;
	
	private String identifier;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

//	public String getOnClass() {
//		return onClass;
//	}
//
//	public void setOnClass(String onClass) {
//		this.onClass = onClass;
//	}

//	public ClassIndexElement getElement() {
//		return element;
//	}
//
//	public void setElement(ClassIndexElement element) {
//		this.element = element;
//	}
//
//	public List<IndexKeyMetadata> getKeysMetadata() {
//		return keysMetadata;
//	}
//
//	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
//		this.keysMetadata = keysMetadata;
//	}

	public Computation getComputation() {
		return computation;
	}

	public void setComputation(Computation computation) {
		this.computation = computation;
	}

	public String getSchemaDatasetId() {
		return schemaDatasetId;
	}

	public void setSchemaDatasetId(String schemaDatasetId) {
		this.schemaDatasetId = schemaDatasetId;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public SchemaSelector getStructure() {
		return structure;
	}

	public void setStructure(SchemaSelector structure) {
		this.structure = structure;
	}

}
