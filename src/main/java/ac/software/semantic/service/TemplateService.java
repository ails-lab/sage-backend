package ac.software.semantic.service;

import ac.software.semantic.model.Template;
import ac.software.semantic.model.TemplateType;
import ac.software.semantic.payload.TemplateDropdownItem;
import ac.software.semantic.payload.TemplateItem;
import ac.software.semantic.repository.TemplateRepository;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class TemplateService {
    @Autowired
    private TemplateRepository templateRepository;

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

    public Template getPredefinedImportTemplate(String predefinedImportName, String predefinedImportType) {
        Optional<Template> tempOpt = templateRepository.findByTypeAndName(TemplateType.PREDEFINED_IMPORT, predefinedImportName + "-" + predefinedImportType);

        return tempOpt.isPresent() ? tempOpt.get() : null;

    }

    public void createImportTemplate(ObjectId creatorId, String name, TemplateType type, String json) {
        Template tmp = new Template();
        tmp.setName(name);
        tmp.setCreatorId(creatorId);
        tmp.setTemplateString(json);
        tmp.setType(type);
        templateRepository.save(tmp);
    }
}
