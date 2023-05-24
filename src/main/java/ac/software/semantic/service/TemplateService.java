package ac.software.semantic.service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.Template;
import ac.software.semantic.model.constants.TemplateType;
import ac.software.semantic.payload.TemplateDropdownItem;
import ac.software.semantic.payload.TemplateItem;
import ac.software.semantic.repository.TemplateRepository;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Service
public class TemplateService {
	
	@Autowired
    @Qualifier("database")
    private Database database;
	
	@Value("${dataservice.definition.folder}")
	private String templateFolder;

    @Autowired
    private TemplateRepository templateRepository;

	@Autowired
	ResourceLoader resourceLoader;

    public List<TemplateDropdownItem> getAllUserTemplates(ObjectId user, TemplateType type) {

        List<Template> templateList = templateRepository.findAllByCreatorIdAndType(user, type);

        return templateList.stream()
                .map(template -> new TemplateDropdownItem(template))
                .collect(Collectors.toList());
    }

    public TemplateItem createTemplate(ObjectId creatorId, String name, TemplateType type, Map<String, Object> templateBody) {
        Template tmp = new Template(creatorId, type, templateBody, name);
        templateRepository.save(tmp);
        return new TemplateItem(tmp);
    }

    public TemplateItem createTemplate(ObjectId creatorId, String name, TemplateType type, String templateString) {
        Template tmp = new Template(creatorId, type, templateString, name);
        templateRepository.save(tmp);
        return new TemplateItem(tmp);
    }

//    public Template getPredefinedImportTemplate(String predefinedImportName, String predefinedImportType) {
//        Optional<Template> tempOpt = templateRepository.findByTypeAndName(TemplateType.DATASET_IMPORT, predefinedImportName + "-" + predefinedImportType);
//
//        return tempOpt.isPresent() ? tempOpt.get() : null;
//
//    }

    public void createImportTemplate(ObjectId creatorId, String name, TemplateType type, String json) {
        Template tmp = new Template();
        tmp.setName(name);
        tmp.setCreatorId(creatorId);
        tmp.setTemplateString(json);
        tmp.setType(type);
        templateRepository.save(tmp);
    }

    public List<Template> getMappingSampleTemplates() {
        return templateRepository.findByTypeAndDatabaseId(TemplateType.MAPPING_SAMPLE, database.getId());
    }

    public List<Template> getDatasetImportTemplates() {
        return templateRepository.findByTypeAndDatabaseId(TemplateType.DATASET_IMPORT, database.getId());
    }

    public List<Template> getCatalogImportTemplates() {
        return templateRepository.findByTypeAndDatabaseId(TemplateType.CATALOG_IMPORT, database.getId());
    }

    public Template getDatasetImportTemplate(String name) {
    	return templateRepository.findByNameAndTypeAndDatabaseId(name, TemplateType.DATASET_IMPORT, database.getId()).orElse(null);
    }

    public Template getDatasetImportTemplate(ObjectId templateId) {
    	return templateRepository.findByIdAndTypeAndDatabaseId(templateId, TemplateType.DATASET_IMPORT, database.getId()).orElse(null);
    }

    public Template getCatalogImportTemplate(ObjectId templateId) {
    	return templateRepository.findByIdAndTypeAndDatabaseId(templateId, TemplateType.CATALOG_IMPORT, database.getId()).orElse(null);
    }

    public List<Template> getDatasetImportTemplateHeaders(ObjectId templateId) {
    	return templateRepository.findByTypeAndTemplateId(TemplateType.DATASET_IMPORT_HEADER, templateId);
    }

    public List<Template> getDatasetImportTemplateContents(ObjectId templateId) {
    	return templateRepository.findByTypeAndTemplateId(TemplateType.DATASET_IMPORT_CONTENT, templateId);
    }

    public Template getDatasetImportContentTemplate(String name, ObjectId templateId) {
    	return templateRepository.findByNameAndTypeAndTemplateId(name, TemplateType.DATASET_IMPORT_CONTENT, templateId).orElse(null);
    }

//    public Template getDatasetImportTemplateHeaders(String templateName) {
//    	Optional<Template> tempOpt = templateRepository.findByTypeAndName(TemplateType.DATASET_IMPORT, predefinedImportName + "-" + predefinedImportType);
//
//    	return tempOpt.isPresent() ? tempOpt.get() : null;
//    }
//}
    
    

	public String getEffectiveTemplateString(Template t) throws Exception {
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