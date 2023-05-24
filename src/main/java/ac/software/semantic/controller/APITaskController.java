package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.Parameter;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.payload.TaskResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.TaskService;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Tasks API")
@RestController
@RequestMapping("/api/tasks")
public class APITaskController {

	Logger logger = LoggerFactory.getLogger(APITaskController.class);

	@Autowired
	private TaskService tasksService;

    @GetMapping("/get")
	public List<TaskResponse> getAll(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @RequestParam(required = false) Date from)  {

    	List<TaskResponse> res = new ArrayList<>();
    	
    	if (from == null) {
	    	from = new Date(System.currentTimeMillis());
	    	
	    	Calendar calendar = Calendar.getInstance();
	    	calendar.setTime(from);
	    	calendar.add(Calendar.DAY_OF_MONTH, -10);
	    	from = calendar.getTime();
    	}
    	
    	List<TaskDescription> children = tasksService.getTasks(currentUser.getId(), from, true);
    	List<TaskDescription> parent = tasksService.getTasks(currentUser.getId(), from, false);
    	
    	Map<ObjectId, TaskDescription> childrenMap = new HashMap<>();
    	for (TaskDescription tdescr : children) {
    		childrenMap.put(tdescr.getId(), tdescr);
    	}
    	for (TaskDescription tdescr : parent) {
    		childrenMap.put(tdescr.getId(), tdescr);
    	}
    	
   		for (TaskDescription tdescr : parent) {
   			res.add(connectChildren(tdescr, childrenMap));
   		}
   		
   		return res;
	}
    
    private TaskResponse connectChildren(TaskDescription tdescr, Map<ObjectId, TaskDescription> childrenMap) {
    	TaskResponse tr = childrenMap.get(tdescr.getId()).toTaskResponse();
    	
		List<ObjectId> list = tdescr.getChildrenIds();
		if (list != null) {
			for (ObjectId ch : list) {
				TaskDescription chdescr = childrenMap.get(ch);
				tr.addChild(connectChildren(chdescr, childrenMap));
			}
		}
		
		return tr;
    }
    


    
    @PostMapping("/stop/{id}")
	public void stop(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable("id") String id)  {

//    	System.out.println("REQUEST STOP " + id);
    	tasksService.requestStop(new ObjectId(id));
	}	
}
