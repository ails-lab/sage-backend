package ac.software.semantic.model.base;

public interface OrderedDocument extends DatasetContained {

	public int getOrder();
	
	public void setOrder(int order);
}
