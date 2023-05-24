package ac.software.semantic.model.state;

public class MappingPublishState extends PublishState {

	private MappingExecuteState execute;

	public MappingPublishState() { 
		super();
	}
	
	public MappingExecuteState getExecute() {
		return execute;
	}

	public void setExecute(MappingExecuteState execute) {
		this.execute = execute;
	}
	   
}	   
