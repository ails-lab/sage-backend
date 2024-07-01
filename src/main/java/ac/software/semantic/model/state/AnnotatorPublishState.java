package ac.software.semantic.model.state;

public class AnnotatorPublishState extends MappingPublishState {

	private String asProperty;

	public AnnotatorPublishState() { 
		super();
	}

	public String getAsProperty() {
		return asProperty;
	}

	public void setAsProperty(String asProperty) {
		this.asProperty = asProperty;
	}
	   
}	   
