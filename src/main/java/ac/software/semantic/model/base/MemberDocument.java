package ac.software.semantic.model.base;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import ac.software.semantic.model.DataDocument;

public interface MemberDocument<M extends DataDocument> extends SpecificationDocument {

	public boolean hasMember(M member);
	
	public void addMember(M member);
	
	public void removeMember(M member);
	
	public List<ObjectId> getMemberIds(Class<? extends M> clazz);
	
	class MemberOperationResult {
		public List<ObjectId> list;
		public boolean flag;
		
		MemberOperationResult(List<ObjectId> list, boolean flag) {
			this.list = list;
			this.flag = flag;
		}
	}
	
	static MemberOperationResult addMemberId(ObjectId id, List<ObjectId> list) {
		if (list == null) {
			list = new ArrayList<>();
		}

		if (list.contains(id)) {
			return new MemberOperationResult(list, false);
		} else {
			list.add(id);
			return new MemberOperationResult(list, true);
		}
	}
	
	static MemberOperationResult removeMemberId(ObjectId id, List<ObjectId> list) {
		boolean removed = false;
		if (list != null) {
			removed = list.remove(id);
		}

		if (list.size() == 0) {
			list = null;
		}

		return new MemberOperationResult(list, removed);
	}
	
	static boolean hasMemberId(ObjectId id, List<ObjectId> list) {
		if (list == null) {
			return false;
		} else {
			return list.contains(id);
		}
	}


}
