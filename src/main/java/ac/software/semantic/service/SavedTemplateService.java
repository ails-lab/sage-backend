package ac.software.semantic.service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.SavedTemplate;
import ac.software.semantic.model.TemplateService;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.TemplateType;
import ac.software.semantic.payload.TemplateDropdownItem;
import ac.software.semantic.payload.TemplateItem;
import ac.software.semantic.repository.core.SavedTemplateRepository;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class SavedTemplateService {
	
	@Autowired
    @Qualifier("database")
    private Database database;
	
	@Value("${dataservice.definition.folder}")
	private String templateFolder;

    @Autowired
    private SavedTemplateRepository templateRepository;

	@Autowired
	ResourceLoader resourceLoader;

    public List<TemplateDropdownItem> getAllUserTemplates(ObjectId user, TemplateType type) {

        List<SavedTemplate> templateList = templateRepository.findAllByCreatorIdAndType(user, type);

        return templateList.stream()
                .map(template -> new TemplateDropdownItem(template))
                .collect(Collectors.toList());
    }

    public TemplateItem createTemplate(ObjectId creatorId, String name, TemplateType type, Map<String, Object> templateMap) {
        SavedTemplate tmp = new SavedTemplate();
        tmp.setCreatorId(creatorId);
        tmp.setType(type);
        tmp.setTemplateMap(templateMap);
        tmp.setName(name);
        
        templateRepository.save(tmp);
        return new TemplateItem(tmp);
    }

    public TemplateItem createTemplate(ObjectId creatorId, String name, TemplateType type, String templateString) {
        SavedTemplate tmp = new SavedTemplate();
        tmp.setCreatorId(creatorId);
        tmp.setType(type);
        tmp.setTemplateString(templateString);
        tmp.setName(name);

        templateRepository.save(tmp);
        return new TemplateItem(tmp);
    }

    public void createImportTemplate(ObjectId creatorId, String name, TemplateType type, String json) {
        SavedTemplate tmp = new SavedTemplate();
        tmp.setName(name);
        tmp.setCreatorId(creatorId);
        tmp.setTemplateString(json);
        tmp.setType(type);
        templateRepository.save(tmp);
    }

//    public List<SavedTemplate> getMappingSampleTemplates() {
//        return templateRepository.findByTypeAndDatabaseId(TemplateType.MAPPING_SAMPLE, database.getId());
//    }
//
//    public List<SavedTemplate> getDatasetImportTemplates() {
//        return templateRepository.findByTypeAndDatabaseId(TemplateType.DATASET_IMPORT, database.getId());
//    }
//
//    public List<SavedTemplate> getCatalogImportTemplates() {
//        return templateRepository.findByTypeAndDatabaseId(TemplateType.CATALOG_IMPORT, database.getId());
//    }
//
//    public SavedTemplate getDatasetImportTemplate(String name) {
//    	return templateRepository.findByNameAndTypeAndDatabaseId(name, TemplateType.DATASET_IMPORT, database.getId()).orElse(null);
//    }
//
//    public SavedTemplate getDatasetImportTemplate(ObjectId templateId, DatasetType type) {
//    	if (type.equals(DatasetType.DATASET)) {
//    		return templateRepository.findByIdAndTypeAndDatabaseId(templateId, TemplateType.DATASET_IMPORT, database.getId()).orElse(null);
//    	} else if (type.equals(DatasetType.CATALOG)) {
//    		return templateRepository.findByIdAndTypeAndDatabaseId(templateId, TemplateType.CATALOG_IMPORT, database.getId()).orElse(null);
//    	}
//    	
//    	return null;
//    }
//
//    public List<SavedTemplate> getDatasetImportTemplateHeaders(ObjectId templateId) {
//    	return templateRepository.findByTypeAndTemplateId(TemplateType.DATASET_IMPORT_HEADER, templateId);
//    }
//
//    public List<SavedTemplate> getDatasetImportTemplateContents(ObjectId templateId) {
//    	return templateRepository.findByTypeAndTemplateId(TemplateType.DATASET_IMPORT_CONTENT, templateId);
//    }
//
//    public SavedTemplate getDatasetImportContentTemplate(String name, ObjectId templateId) {
//    	return templateRepository.findByNameAndTypeAndTemplateId(name, TemplateType.DATASET_IMPORT_CONTENT, templateId).orElse(null);
//    }

	public String getEffectiveTemplateString(TemplateService t) throws Exception {
		if (t.getEffectiveTemplateString() == null) {
			if (t.getTemplateString() != null) {
				t.setEffectiveTemplateString(t.getTemplateString());
			} else if (t.getTemplateFile() != null) {
				try (InputStream inputStream = resourceLoader.getResource("classpath:" + templateFolder + t.getTemplateFile()).getInputStream()) {
					t.setEffectiveTemplateString(new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8));
				}				
			}
		}
		
		return t.getEffectiveTemplateString();
	}

}