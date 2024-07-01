package ac.software.semantic.payload;

import ac.software.semantic.model.SavedTemplate;
import ac.software.semantic.model.constants.type.TemplateType;

import java.util.Map;

public class TemplateItem {
    private String id;
    private TemplateType type;
    private String name;
    private Map<String, Object> templateJson;
    private String templateString;
    public TemplateItem(SavedTemplate tmp) {
        this.id = tmp.getId().toString();
        this.type = tmp.getType();
        this.name = tmp.getName();
        this.templateJson = tmp.getTemplateMap();
        this.templateString = tmp.getTemplateString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TemplateType getType() {
        return type;
    }

    public void setType(TemplateType type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getTemplateJson() {
        return templateJson;
    }

    public void setTemplateJson(Map<String, Object> templateJson) {
        this.templateJson = templateJson;
    }

    public String getTemplateString() {
        return templateString;
    }

    public void setTemplateString(String templateString) {
        this.templateString = templateString;
    }
}
