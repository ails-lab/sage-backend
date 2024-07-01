package ac.software.semantic.payload;

import ac.software.semantic.model.SavedTemplate;
import org.bson.types.ObjectId;

public class TemplateDropdownItem {
    private String id;
    private String name;
    private String templateString;

    public TemplateDropdownItem(SavedTemplate template) {
        this.id = template.getId().toString();
        this.name = template.getName();
        this.templateString = template.getTemplateString();
    }

    public TemplateDropdownItem(SavedTemplate template, String templateString) {
        this.id = template.getId().toString();
        this.name = template.getName();
        this.templateString = templateString;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTemplateString() {
        return templateString;
    }

    public void setTemplateString(String templateString) {
        this.templateString = templateString;
    }
}
