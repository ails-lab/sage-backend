package ac.software.semantic.model.state;

public class IndexState extends CreateState {

	private PublishState publish;

	public IndexState() {
		super();
	}
	
	public PublishState getPublish() {
		return publish;
	}

	public void setPublish(PublishState publish) {
		this.publish = publish;
	}
}
