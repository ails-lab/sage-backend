package ac.software.semantic.model.state;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.VocabularyEntityDescriptor;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.payload.response.ResponseTaskObject;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasetPublishState extends MappingPublishState {

	private List<String> uriSpaces;
	
	private List<DatasetPublishState> groups;
	
	private Integer group;
	
	public DatasetPublishState() { 
		super();
	}

	public List<String> getUriSpaces() {
		return uriSpaces;
	}

	public void setUriSpaces(List<String> uriSpaces) {
		this.uriSpaces = uriSpaces;
	}

	@Override
	public ResponseTaskObject createResponseState() {
		ResponseTaskObject res = super.createResponseState();
   		res.setUriSpaces(uriSpaces);

   		if (groups != null) {
   			List<ResponseTaskObject> rgroups = new ArrayList<>();
   			for (DatasetPublishState ps : groups) {
   				ResponseTaskObject r = ps.createResponseState();
   				r.setGroup(ps.group);
   				
   				rgroups.add(r);
   			}
   			
   			res.setGroups(rgroups);
   		}
   		
    	return res;
	}
	
	public List<VocabularyEntityDescriptor> namespaces2UriDesciptors() {
		List<VocabularyEntityDescriptor> veds = new ArrayList<>();
		if (uriSpaces != null) {
			for (String s : uriSpaces) {
				VocabularyEntityDescriptor ved = new VocabularyEntityDescriptor();
				ved.setNamespace(s);
				veds.add(ved);
			}
		}
		return veds;
	}
	
	public void removeGroup(int group) {
		if (groups != null) {
			for (int i = 0; i < groups.size(); i++) {
				DatasetPublishState gr = groups.get(i);
				if (gr.getGroup() == group) {
					groups.remove(i);
					break;
				}
			}
			
			if (groups.size() == 0) {
				groups = null;
			}
		}
	}

	public List<DatasetPublishState> getGroups() {
		return groups;
	}

	public void setGroups(List<DatasetPublishState> groups) {
		this.groups = groups;
	}

	public Integer getGroup() {
		return group;
	}

	public void setGroup(Integer group) {
		this.group = group;
	}

	public void startDo(TaskMonitor tm, Integer group) {
		if (group != null) {
			
			if (groups == null) {
				groups = new ArrayList<>();
			}

			DatasetPublishState gps = null;
			
			for (int k = 0; k < groups.size(); k++) {
				if (groups.get(k).group == group) {
					gps = groups.get(k);
					break;
				}
			}
			
			if (gps == null) {
				gps = new DatasetPublishState();
				gps.setGroup(group);
				
				groups.add(gps);
			}
			
			gps.setPublishState(DatasetState.PUBLISHING);
			gps.setPublishStartedAt(new Date());
			gps.setPublishCompletedAt(null);
			gps.clearMessages();
		}
	}
	
	public void completeDo(TaskMonitor tm, Integer group) {
		if (group != null) {
			
			if (groups == null) {
				groups = new ArrayList<>();
			}

			DatasetPublishState gps = null;
			
			for (int k = 0; k < groups.size(); k++) {
				if (groups.get(k).group == group) {
					gps = groups.get(k);
					break;
				}
			}
			
			if (gps == null) {
				gps = new DatasetPublishState();
				gps.setGroup(group);
				
				groups.add(gps);
			}
			
			gps.setPublishState(DatasetState.PUBLISHED);
			gps.setPublishCompletedAt(new Date());
		}
	}
	
	public void startUndo(TaskMonitor tm, Integer group) {
		if (group != null) {
			
			if (groups == null) {
				groups = new ArrayList<>();
			}

			DatasetPublishState gps = null;
			
			for (int k = 0; k < groups.size(); k++) {
				if (groups.get(k).group == group) {
					gps = groups.get(k);
					break;
				}
			}
			
			if (gps == null) {
				gps = new DatasetPublishState();
				gps.setGroup(group);
				
				groups.add(gps);
			}
			
			gps.setPublishState(DatasetState.UNPUBLISHING);
			gps.setPublishStartedAt(new Date());
			gps.setPublishCompletedAt(null);
			gps.clearMessages();
		}
	}
	
	
	public void failDo(TaskMonitor tm, Integer group) {
		if (group != null) {
			
			if (groups == null) {
				groups = new ArrayList<>();
			}

			DatasetPublishState gps = null;
			
			for (int k = 0; k < groups.size(); k++) {
				if (groups.get(k).group == group) {
					gps = groups.get(k);
					break;
				}
			}
			
			if (gps == null) {
				gps = new DatasetPublishState();
				gps.setGroup(group);
				
				groups.add(gps);
			}
			
			gps.setPublishState(DatasetState.PUBLISHING_FAILED);
			gps.setPublishCompletedAt(new Date());
			gps.setMessage(tm.getFailureMessage());
		}

	}
	
	public void failUndo(TaskMonitor tm, Integer group) {
		if (group != null) {
			
			if (groups == null) {
				groups = new ArrayList<>();
			}

			DatasetPublishState gps = null;
			
			for (int k = 0; k < groups.size(); k++) {
				if (groups.get(k).group == group) {
					gps = groups.get(k);
					break;
				}
			}
			
			if (gps == null) {
				gps = new DatasetPublishState();
				gps.setGroup(group);
				
				groups.add(gps);
			}
			
			gps.setPublishState(DatasetState.UNPUBLISHING_FAILED);
			gps.setPublishCompletedAt(new Date());
			gps.setMessage(tm.getFailureMessage());
		}		
	}
}	   
