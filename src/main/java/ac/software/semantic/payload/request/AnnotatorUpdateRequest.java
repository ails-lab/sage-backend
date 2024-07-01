package ac.software.semantic.payload.request;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import ac.software.semantic.model.ControlProperty;
import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.PathElement;
import ac.software.semantic.model.PreprocessInstruction;
import ac.software.semantic.model.SchemaSelector;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexElementSelector;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.index.PropertyIndexElement;
import edu.ntua.isci.ac.common.utils.Counter;

public class AnnotatorUpdateRequest implements UpdateRequest, InGroupRequest {

	private List<PathElement> onPath;
	
	private String onClass;
	
	private String name;
	private String identifier;
	
	private String asProperty;
	private String annotator;   //system annotator
	private String annotatorId; //user annotator
	
//	private String thesaurus;
	private String thesaurusId;
	
	private String variant;
	
	private String defaultTarget;
	
	private List<String> tags;
	
	private List<DataServiceParameterValue> parameters;
	private List<PreprocessInstruction> preprocess;
	
//	private ClassIndexElement indexStructure;
//	private List<IndexKeyMetadata> keysMetadata;
	private SchemaSelector structure;
	
	private List<String> bodyProperties;
	private SchemaSelector control;
	
	private int group;
	
	public List<PathElement> getOnPath() {
		return onPath;
	}
	
	public void setOnPath(List<PathElement> onPath) {
		this.onPath = onPath;
	}
	
	public String getAsProperty() {
		return asProperty;
	}
	
	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}
	
	public String getAnnotator() {
		return annotator;
	}
	
	public void setAnnotator(String annotator) {
		this.annotator = annotator;
	}
	
//	public String getThesaurus() {
//		return thesaurus;
//	}
//	
//	public void setThesaurus(String thesaurus) {
//		this.thesaurus = thesaurus;
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
	
	public String getDefaultTarget() {
		return defaultTarget;
	}
	
	public void setDefaultTarget(String defaultTarget) {
		this.defaultTarget = defaultTarget;
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

//	public ClassIndexElement getIndexStructure() {
//		return indexStructure;
//	}
//
//	public void setIndexStructure(ClassIndexElement indexStructure) {
//		this.indexStructure = indexStructure;
//	}
//
//	public List<IndexKeyMetadata> getKeysMetadata() {
//		return keysMetadata;
//	}
//
//	public void setKeysMetadata(List<IndexKeyMetadata> keysMetadata) {
//		this.keysMetadata = keysMetadata;
//	}
	
	public void normalize() {
		if (structure == null || structure.getKeysMetadata() == null) {
			return;
		}
		
		Map<Integer, Integer> mapping = new HashMap<>();
		
		List<String> keyNames = new ArrayList<>();
		
		for (IndexKeyMetadata ikm : structure.getKeysMetadata()) {
			keyNames.add(ikm.getName());
		}
		
		Collections.sort(keyNames);
		
		ArrayList<IndexKeyMetadata> newIkm = new ArrayList<>(); 
		
		// map old indices to new indices according to key name sorting
		for (int i = 0; i < keyNames.size(); i++) {
			String keyName = keyNames.get(i);
			
			for (IndexKeyMetadata ikm : structure.getKeysMetadata()) {
				if (ikm.getName().equals(keyName)) {
					mapping.put(ikm.getIndex(), i);
					ikm.setIndex(i);
					newIkm.add(ikm);
					break;
				}
			}	
		}
		
		structure.setKeysMetadata(newIkm);
		
		reassignIndices(structure.getElement(), mapping, new Counter(mapping.size()));

	}
	
	private void reassignIndices(ClassIndexElement cie, Map<Integer, Integer> mapping, Counter noKeyIndex) {
		for (PropertyIndexElement pie : cie.getProperties()) {
			if (pie.getSelectors() != null) {
				for (IndexElementSelector ies : pie.getSelectors()) {
					Integer newIndex = mapping.get(ies.getIndex());
					if (newIndex != null) {
						ies.setIndex(newIndex);
					} else { // for indices not in keymetadata : selectors specifying only values not return variables
						ies.setIndex(noKeyIndex.getValue()); 
						noKeyIndex.increase();
					}
				}
			}
			
			if (pie.getElements() != null) {
				for (ClassIndexElement pcie : pie.getElements()) {
					reassignIndices(pcie, mapping, noKeyIndex);
				}
			}
		}
	}

	public String getThesaurusId() {
		return thesaurusId;
	}

	public void setThesaurusId(String thesaurusId) {
		this.thesaurusId = thesaurusId;
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

	@Override
	public int getGroup() {
		return group;
	}

	@Override
	public void setGroup(int group) {
		this.group = group;
	}

	public SchemaSelector getControl() {
		return control;
	}

	public void setControl(SchemaSelector control) {
		this.control = control;
	}

	public SchemaSelector getStructure() {
		return structure;
	}

	public void setStructure(SchemaSelector structure) {
		this.structure = structure;
	}


}
 