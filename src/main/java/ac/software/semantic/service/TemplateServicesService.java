package ac.software.semantic.service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.SavedTemplate;
import ac.software.semantic.model.TemplateService;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.TemplateType;
import ac.software.semantic.payload.TemplateDropdownItem;
import ac.software.semantic.payload.TemplateItem;
import ac.software.semantic.repository.core.SavedTemplateRepository;
import ac.software.semantic.repository.root.TemplateServiceRepository;

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
public class TemplateServicesService {
	
	@Autowired
    @Qualifier("database")
    private Database database;
	
	@Value("${dataservice.definition.folder}")
	private String templateFolder;

    @Autowired
    private TemplateServiceRepository templateRepository;

	@Autowired
	private ResourceLoader resourceLoader;

    public List<TemplateService> getMappingSampleTemplates() {
        return templateRepository.findByTypeAndDatabaseIdOrderByName(TemplateType.MAPPING_SAMPLE, database.getId());
    }

    public List<TemplateService> getDatasetImportTemplates() {
        return templateRepository.findByTypeAndDatabaseIdOrderByName(TemplateType.DATASET_IMPORT, database.getId());
    }

    public List<TemplateService> getCatalogImportTemplates() {
        return templateRepository.findByTypeAndDatabaseIdOrderByName(TemplateType.CATALOG_IMPORT, database.getId());
    }

    public TemplateService getDatasetImportTemplate(String name) {
    	return templateRepository.findByNameAndTypeAndDatabaseId(name, TemplateType.DATASET_IMPORT, database.getId()).orElse(null);
    }

    public TemplateService getDatasetImportTemplate(ObjectId templateId, DatasetType type) {
    	if (type.equals(DatasetType.DATASET)) {
    		return templateRepository.findByIdAndTypeAndDatabaseId(templateId, TemplateType.DATASET_IMPORT, database.getId()).orElse(null);
    	} else if (type.equals(DatasetType.CATALOG)) {
    		return templateRepository.findByIdAndTypeAndDatabaseId(templateId, TemplateType.CATALOG_IMPORT, database.getId()).orElse(null);
    	}
    	
    	return null;
    }

    public List<TemplateService> getDatasetImportTemplateHeaders(ObjectId templateId) {
    	return templateRepository.findByTypeAndTemplateIdOrderByName(TemplateType.DATASET_IMPORT_HEADER, templateId);
    }

    public List<TemplateService> getDatasetImportTemplateContents(ObjectId templateId) {
    	return templateRepository.findByTypeAndTemplateIdOrderByName(TemplateType.DATASET_IMPORT_CONTENT, templateId);
    }

    public TemplateService getDatasetImportContentTemplate(String name, ObjectId templateId) {
    	return templateRepository.findByNameAndTypeAndTemplateId(name, TemplateType.DATASET_IMPORT_CONTENT, templateId).orElse(null);
    }

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