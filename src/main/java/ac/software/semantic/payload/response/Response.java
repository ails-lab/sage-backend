package ac.software.semantic.payload.response;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.base.CreatableDocument;
import ac.software.semantic.model.base.MappingExecutePublishDocument;
import ac.software.semantic.model.base.PublishableDocument;
import ac.software.semantic.model.base.RunnableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.base.StartableDocument;
import ac.software.semantic.model.base.ValidatableDocument;
import ac.software.semantic.model.constants.state.CreatingState;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.RunningState;
import ac.software.semantic.model.constants.state.ValidatingState;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.state.RunState;
import ac.software.semantic.model.state.ValidateState;

public interface Response {

	public default void copyStates(SpecificationDocument doc, TripleStoreConfiguration vc, FileSystemConfiguration fc) {
		
    	if (doc instanceof MappingExecutePublishDocument) {
    		MappingExecutePublishDocument<?,?> idoc = (MappingExecutePublishDocument<?,?>)doc;

        	ExecuteState es = idoc.checkExecuteState(fc.getId());
        	if (es != null) {
        		((ExecuteResponse)this).setExecuteState(es.createResponseState());
        	} else {
        		ResponseTaskObject rto = new ResponseTaskObject();
        		rto.setState(MappingState.NOT_EXECUTED.toString());
        		((ExecuteResponse)this).setExecuteState(rto);
        	}

        	PublishState<?> ps = (vc != null) ? idoc.checkPublishState(vc.getId()) : null;
        	if (ps != null) {
	    		ResponseTaskObject rto = ps.createResponseState();
	    		rto.setTripleStoreName(vc.getName());
        		((PublishResponse)this).setPublishState(rto);
        	} else {
        		ResponseTaskObject rto = new ResponseTaskObject();
        		rto.setState(DatasetState.UNPUBLISHED.toString());
        		((PublishResponse)this).setPublishState(rto);
        	}
        	
        	adjustResponse((ExecutePublishResponse)this, ps, es, fc);

    	} else if (doc instanceof PublishableDocument) {
    		PublishableDocument<?,?> idoc = (PublishableDocument<?,?>)doc;
    		
    		PublishState<?> ps = (vc != null) ? idoc.checkPublishState(vc.getId()) : null;
	    	if (ps != null) {
	    		ResponseTaskObject rto = ps.createResponseState();
	    		rto.setTripleStoreName(vc.getName());
	    		((PublishResponse)this).setPublishState(rto);
	    	} else {
        		ResponseTaskObject rto = new ResponseTaskObject();
        		rto.setState(DatasetState.UNPUBLISHED.toString());
        		((PublishResponse)this).setPublishState(rto);
        	}

    	}
    	
    	if (doc instanceof ValidatableDocument) {
    		ValidatableDocument<?> idoc = (ValidatableDocument<?>)doc;

    		ValidateState vs = idoc.checkValidateState(fc.getId());
    		if (vs != null) {
    			((ValidateResponse)this).setValidateState(vs.createResponseState());
    		} else {
        		ResponseTaskObject rto = new ResponseTaskObject();
        		rto.setState(ValidatingState.NOT_VALIDATED.toString());
        		((ValidateResponse)this).setValidateState(rto);
    		}
    	}
    	
    	if (doc instanceof RunnableDocument) {
    		RunnableDocument idoc = (RunnableDocument)doc;

    		RunState rs = idoc.checkRunState(fc.getId());
    		if (rs != null) {
    			((RunResponse)this).setRunState(rs.createResponseState());
    		} else {
        		ResponseTaskObject rto = new ResponseTaskObject();
        		rto.setState(RunningState.NOT_RUNNING.toString());
        		((RunResponse)this).setRunState(rto);
    		}
    			
    	}
    	
    	if (doc instanceof CreatableDocument) {
    		CreatableDocument<?> idoc = (CreatableDocument<?>)doc;
    		
    		CreateState cs = idoc.checkCreateState(idoc.getElasticConfigurationId(), fc.getId());
    		if (cs != null) {
    			((CreateResponse)this).setCreateState(cs.createResponseState());
    		} else {
        		ResponseTaskObject rto = new ResponseTaskObject();
        		rto.setState(CreatingState.NOT_CREATED.toString());
        		((CreateResponse)this).setCreateState(rto);
    		}
    	}
    	
    	if (doc instanceof StartableDocument) {
    		StartableDocument idoc = (StartableDocument)doc;
    		
    		((LifecycleResponse)this).setLifecycleState(idoc.createResponseState());
    	}
    }
	
    public default void adjustResponse(ExecutePublishResponse res, PublishState<?> ps, ExecuteState es, FileSystemConfiguration fc) {
    	if (ps != null) { 
	    	ExecuteState pes = ps.getExecute();
	
	    	if (pes != null) {
	
	    		if (pes.getDatabaseConfigurationId().equals(fc.getId())) {
		    		res.setPublishedFromCurrentFileSystem(true);
		    		
		    		if (es != null && pes.getExecuteStartedAt().equals(es.getExecuteStartedAt())) {
		    			res.setNewExecution(false);
		    		} else {
		    			res.setNewExecution(true);
		    		}
		    	} else {
		    		res.setPublishedFromCurrentFileSystem(false);
		    	}
	    	}
    	}
    }
}
