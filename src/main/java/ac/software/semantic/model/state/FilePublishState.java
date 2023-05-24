package ac.software.semantic.model.state;

public class FilePublishState extends PublishState {

	private FileExecuteState execute;

	public FilePublishState() { 
		super();
	}
	
	public FileExecuteState getExecute() {
		return execute;
	}

	public void setExecute(FileExecuteState execute) {
		this.execute = execute;
	}
	   
}	   
