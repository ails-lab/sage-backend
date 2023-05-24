package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.List;

public class DatasetCatalog {
	private Dataset dataset;
	private List<Dataset> members;
	
	public DatasetCatalog(Dataset dataset) {
		this.dataset = dataset;
		members = new ArrayList<>();
	}
	
	public Dataset getDataset() {
		return dataset;
	}
	
	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}
	
	public List<Dataset> getMembers() {
		return members;
	}
	
	public void setMembers(List<Dataset> members) {
		this.members = members;
	}
	
	public void addMember(Dataset member) {
		this.members.add(member);
	}
	
}
