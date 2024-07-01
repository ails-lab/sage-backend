package ac.software.semantic.model;

import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "TemplateServices")
public class TemplateService extends Template {

    public TemplateService() {
    	super();
    };
}
