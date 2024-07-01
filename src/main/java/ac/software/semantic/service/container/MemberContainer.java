package ac.software.semantic.service.container;

import java.util.List;

import ac.software.semantic.model.DataDocument;
import ac.software.semantic.model.base.MemberDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.ContainerService;

public interface MemberContainer<D extends MemberDocument, F extends Response, M extends DataDocument> extends BaseContainer<D,F> {
	
	public default void addMember(M member) throws Exception {
		update(idc -> {
			D ds = idc.getObject();
			ds.addMember(member);
		});
	}
	
	public default void removeMember(M member) throws Exception {
		update(idc -> {
			D ds = idc.getObject();
			ds.removeMember(member);
		});
	}
	
	public default boolean hasMember(M member) throws Exception {
		return getObject().hasMember(member);
	}

}
