package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.TemplateType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.Transient;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Template")
public class Template {

    @Id
    private ObjectId id;
    
    private ObjectId creatorId;
    private ObjectId templateId;
    
    private List<ObjectId> databaseId;
    
    private TemplateType type;
    private Map<String, Object> templateMap;
    private String name;
    
    private String templateString;
    private String templateFile;
    
   @Transient
    private String effectiveTemplateString;
    
    private List<ExtendedParameter> parameters;
    
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

	public List<ExtendedParameter> getParameters() {
		return parameters;
	}
	
	public List<String> getParameterNames() {
		List<String> res = new ArrayList<>();
		for (ExtendedParameter s : parameters) {
			res.add(s.getName());
		}
		return res;
	}

	public void setParameters(List<ExtendedParameter> parameters) {
		this.parameters = parameters;
	}

	public ObjectId getTemplateId() {
		return templateId;
	}

	public void setTemplateId(ObjectId templateId) {
		this.templateId = templateId;
	}

	public List<ObjectId> getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(List<ObjectId> databaseId) {
		this.databaseId = databaseId;
	}

	public String getTemplateFile() {
		return templateFile;
	}

	public void setTemplateFile(String templateFile) {
		this.templateFile = templateFile;
	}

	public String getEffectiveTemplateString() {
		return effectiveTemplateString;
	}

	public void setEffectiveTemplateString(String effectiveTemplateString) {
		this.effectiveTemplateString = effectiveTemplateString;
	}


}
