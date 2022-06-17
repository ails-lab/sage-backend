package ac.software.semantic.model;

import nonapi.io.github.classgraph.json.Id;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

@Document(collection = "Template")
public class Template {

    @Id
    private ObjectId id;
    private ObjectId creatorId;
    private TemplateType type;
    private Map<String, Object> templateMap;
    private String name;

    private String templateString;

    public Template(ObjectId creatorId, TemplateType type, Map<String, Object> templateMap, String name) {
        this.creatorId = creatorId;
        this.type = type;
        this.templateMap = templateMap;
        this.name = name;
    }

    public Template(ObjectId creatorId, TemplateType type, String templateString, String name) {
        this.creatorId = creatorId;
        this.type = type;
        this.templateString = templateString;
        this.name = name;
    }

    public Template() {};

    public String getTemplateString() {
        return templateString;
    }

    public void setTemplateString(String templateString) {
        this.templateString = templateString;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ObjectId getId() {
        return id;
    }

    public Map<String, Object> getTemplateMap() {
        return templateMap;
    }

    public void setTemplateMap(Map<String, Object> templateMap) {
        this.templateMap = templateMap;
    }

    public ObjectId getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(ObjectId creatorId) {
        this.creatorId = creatorId;
    }

    public TemplateType getType() {
        return type;
    }

    public void setType(TemplateType type) {
        this.type = type;
    }
}
