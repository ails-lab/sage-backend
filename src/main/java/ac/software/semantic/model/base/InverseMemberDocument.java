package ac.software.semantic.model.base;

import ac.software.semantic.model.DataDocument;

public interface InverseMemberDocument<M extends DataDocument> extends SpecificationDocument {

	public boolean isMemberOf(M target);
	
	public void addTo(M target);
	
	public void removeFrom(M target);
}
