package ac.software.semantic.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.WebSocketService;
import ac.software.semantic.controller.utils.AsyncUtils;
import ac.software.semantic.controller.utils.FileUtils;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.TaskType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.MappingState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.APIResponse;
import ac.software.semantic.payload.PublishNotificationObject;
import ac.software.semantic.security.UserPrincipal;

@Service
public class ServiceUtils {

	Logger logger = LoggerFactory.getLogger(ServiceUtils.class);
	
	@Autowired
	private FolderService folderService;

	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();

		PublishableContainer pc = (PublishableContainer)tdescr.getContainer();
		
		try {
			pc.save(ioc -> {
				PublishState ips = ((PublishableContainer)ioc).getPublishState();
				ips.startDo(pm);
			});
	
			pm.sendMessage(new PublishNotificationObject(pc));
	
			pc.publish();

			pm.complete();

			pc.save(ioc -> {
				MappingPublishState ips = (MappingPublishState)((PublishableContainer)ioc).getPublishState();
				ips.completeDo(pm);
				ips.setExecute(((ExecutableContainer)ioc).getExecuteState());
			});

			logger.info("Publication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " completed.");

			pm.sendMessage(new PublishNotificationObject(pc));
		
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);

			try {
				pc.save(ioc -> {
					PublishState ips = ((PublishableContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm);
					}
				});
				
				if (pc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(pc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}	
	
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();	

		ObjectContainer oc = tdescr.getContainer();
		
		PublishableContainer pc = (PublishableContainer)oc;
		
		try {
			pc.save(ioc -> {
				PublishState ips = ((PublishableContainer)ioc).getPublishState();
				ips.startUndo(pm);
			});
	
			pm.sendMessage(new PublishNotificationObject(pc));
			
			pc.unpublish();

			pm.complete();
			
			oc.save(ioc -> {
				PublishableContainer ipc = (PublishableContainer)ioc;
				ExecutableContainer iec = (ExecutableContainer)ioc;
				
				MappingPublishState ips = (MappingPublishState)ipc.getPublishState();
				
				ipc.removePublishState(ips);
				
				MappingExecuteState ies = iec.getExecuteState();
				MappingExecuteState ipes = ips.getExecute();
				if (ies != null && ipes != null && ies.getExecuteStartedAt().compareTo(ipes.getExecuteStartedAt()) != 0) {
					iec.clearExecution(ipes);
				}
			});

			logger.info("Unpublication of " + oc.getClass().getName() + ":"  + oc.getPrimaryId() + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(pc));
		
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);

			try {
				pc.save(ioc -> {
					PublishState ips = ((PublishableContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failUndo(pm);
					}
				});
				
				if (pc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(pc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	public boolean clearExecution(ObjectContainer oc) throws Exception {
		MappingExecuteState es = ((ExecutableContainer)oc).checkExecuteState();
		
		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
			return false;
		}
		
		return clearExecution(oc, es);
	}
	
	public boolean clearExecution(ObjectContainer oc, MappingExecuteState es) throws Exception {
		
		PublishableContainer pc = (PublishableContainer)oc;
		ExecutableContainer ec = (ExecutableContainer)oc;
		
		ProcessStateContainer psv = pc.getCurrentPublishState();
		
		if (psv != null) {
			MappingPublishState ps = (MappingPublishState)psv.getProcessState();
			MappingExecuteState pes = ps.getExecute();
	
			// do not clear published execution
			if (pes != null && pes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) == 0 && pes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {	
				return false;
			} 
		}
		
		// trig files
		if (es.getExecuteShards() != null) {
			for (int i = 0; i < es.getExecuteShards(); i++) {
				try {
					FileUtils.deleteFile(folderService.getExecutionTrigFile(ec, es, i));
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
		
		try {
			FileUtils.deleteFile(folderService.getExecutionZipFile(ec, es));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		folderService.deleteContainingFolderIfEmpty(oc);
		
		if (ec.checkExecuteState() == es) {
			oc.save(ioc -> {
				PublishableContainer ipc = (PublishableContainer)ioc;
				ExecutableContainer iec = (ExecutableContainer)ioc;
				
				iec.deleteExecuteState();
			
				ProcessStateContainer ipsv = ipc.getCurrentPublishState();
				
				if (ipsv != null) {
					MappingPublishState ips = (MappingPublishState)ipsv.getProcessState();
					MappingExecuteState ipes = ips.getExecute();
			
					// do not clear published execution
					if (ipes != null && ipes.getExecuteStartedAt().compareTo(es.getExecuteStartedAt()) != 0 && ipes.getDatabaseConfigurationId().equals(es.getDatabaseConfigurationId())) {
						MappingExecuteState ines = iec.getExecuteState();
						ines.setCount(ipes.getCount());
						ines.setExecuteCompletedAt(ipes.getExecuteCompletedAt());
						ines.setExecuteStartedAt(ipes.getExecuteStartedAt());
						ines.setExecuteShards(ipes.getExecuteShards());
						ines.setExecuteState(ipes.getExecuteState());
					}
				}
			});
		} 
		
		return true;
	}

	
	public File zipExecution(ExecutableContainer ec, int shards) throws IOException {
		return zipExecution(ec, ec.getExecuteState(), shards);
	}	
	
	public File zipExecution(ExecutableContainer ec, ExecuteState es, int shards) throws IOException {
		
		File file = folderService.createExecutionZipFile(ec, es);
		
		try (FileOutputStream fos = new FileOutputStream(file);
				ZipOutputStream zipOut = new ZipOutputStream(fos)) {
			
			for (int i = 0; i < shards; i++) {
	        	File fileToZip = folderService.getExecutionTrigFile(ec, es, i);

	        	try (FileInputStream fis = new FileInputStream(fileToZip)) {
		            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
		            zipOut.putNextEntry(zipEntry);
		 
		            byte[] bytes = new byte[1024];
		            int length;
		            while((length = fis.read(bytes)) >= 0) {
		                zipOut.write(bytes, 0, length);
		            }
	            }
	        }
        }
		
		return file;
	}
	
}
