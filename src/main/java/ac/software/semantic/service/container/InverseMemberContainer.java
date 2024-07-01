package ac.software.semantic.service.container;

import ac.software.semantic.model.DataDocument;
import ac.software.semantic.model.base.InverseMemberDocument;
import ac.software.semantic.payload.response.Response;

public interface InverseMemberContainer<D extends InverseMemberDocument<M>, F extends Response, M extends DataDocument> extends BaseContainer<D,F> {
	
	public default void addTo(M target) throws Exception {
		update(idc -> {
			D ds = idc.getObject();
			ds.addTo(target);
		});
	}
	
	public default void removeFrom(M target) throws Exception {
		update(idc -> {
			D ds = idc.getObject();
			ds.removeFrom(target);
		});
	}
	
	public default boolean isMemberOf(M target) throws Exception {
		return getObject().isMemberOf(target);
	}

}
