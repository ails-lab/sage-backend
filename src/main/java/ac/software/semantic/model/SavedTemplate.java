package ac.software.semantic.model;

import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Template")
public class SavedTemplate extends Template {

    public SavedTemplate() {
    	super();
    };
}
