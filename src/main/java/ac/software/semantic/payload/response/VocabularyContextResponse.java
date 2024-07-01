package ac.software.semantic.payload.response;

import java.util.List;

import org.springframework.data.annotation.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.Vocabulary;
import ac.software.semantic.model.VocabularyEntityDescriptor;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.SchemaService;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.RDFSVocabulary;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class VocabularyContextResponse {
	
	private String name;
	
	private String id;
	
	private List<VocabularyEntityDescriptor> uriDescriptors;
	
	public VocabularyContextResponse() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<VocabularyEntityDescriptor> getUriDescriptors() {
		return uriDescriptors;
	}

	public void setUriDescriptors(List<VocabularyEntityDescriptor> uriDescriptors) {
		this.uriDescriptors = uriDescriptors;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public String getId() {
		return id;
	}
	
	public static VocabularyContextResponse create(ResourceContext voc) {
		VocabularyContextResponse res = new  VocabularyContextResponse();
		res.setId(voc.getId().toString());
		res.setName(voc.getName());
		res.setUriDescriptors(voc.getUriDescriptors());
		
		return res;
	}

}