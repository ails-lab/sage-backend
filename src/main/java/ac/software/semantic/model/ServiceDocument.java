package ac.software.semantic.model;

import ac.software.semantic.model.DataService.DataServiceType;

public interface ServiceDocument extends SpecificationDocument {
	
	public String getVariant();
	
	public String getIdentifier();
	
	public DataServiceType getType();

}
