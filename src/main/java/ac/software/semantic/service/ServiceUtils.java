package ac.software.semantic.service;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.multipart.MultipartFile;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.graph.GraphFactory;

import ac.software.semantic.controller.utils.FileUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TaskMonitor;
import ac.software.semantic.model.ValidationResult;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.notification.NotificationType;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.state.RunningState;
import ac.software.semantic.model.constants.type.FileType;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.FileExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.state.RunState;
import ac.software.semantic.model.state.ValidateState;
import ac.software.semantic.payload.notification.ExecuteNotificationObject;
import ac.software.semantic.payload.notification.NotificationObject;
import ac.software.semantic.payload.notification.PublishNotificationObject;
import ac.software.semantic.payload.notification.ValidateNotificationObject;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.UserService.UserContainer;
import ac.software.semantic.service.container.DataServiceContainer;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.RunnableContainer;
import ac.software.semantic.service.container.SideExecutableContainer;
import ac.software.semantic.service.container.ValidatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.monitor.ExecuteMonitor;
import ac.software.semantic.service.monitor.GenericMonitor;
import ac.software.semantic.vocs.SEMRVocabulary;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import edu.ntua.isci.ac.common.db.rdf.RDFLibrary;
import edu.ntua.isci.ac.d2rml.model.D2RMLModel;
import edu.ntua.isci.ac.d2rml.output.FileSystemRDFOutputHandler;
import edu.ntua.isci.ac.d2rml.processor.Executor;

import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

@Service
public class ServiceUtils {

	private Logger logger = LoggerFactory.getLogger(ServiceUtils.class);
	
	@Autowired
	private FolderService folderService;

//	@Lazy
	@Autowired
	private UserService userService;
	
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;
	
	@Lazy
	@Autowired
	private SEMRVocabulary resourceVocabulary;

	@Value("${d2rml.execute.request-cache-size}")
	private int restCacheSize;

	@Value("${d2rml.extract.min-size:0}")
	private long extractMinSize; 
	
	@Value("${d2rml.execute.safe}")
	private boolean safeExecute;

	@Value("${d2rml.execute.shard-size}")
	private int shardSize;
	
	public void setObjectOwner(ObjectContainer<?, ?> oc) {
		if (oc.getCurrentUser() != null) {
			 if (oc.getCurrentUser().getId().equals(oc.getObject().getUserId().toString())) {
				 oc.setObjectCreator(oc.getCurrentUser());
			 } 
		}
		
		if (oc.getObjectCreator() == null) {
			UserContainer uc = userService.getContainer(null, new SimpleObjectIdentifier(oc.getObject().getUserId()));
			oc.setObjectCreator(uc.asUserPrincipal());
		}
		
	}
	
	public ListenableFuture<Date> execute(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		ExecuteMonitor em = (ExecuteMonitor)tdescr.getMonitor();

		DataServiceContainer<?,?,?,Dataset> oc = (DataServiceContainer)tdescr.getContainer(); 
		
		try {
			Date executeStart = new Date(System.currentTimeMillis());
			
			clearExecution(oc);
			
			oc.update(iac -> {	
				MappingExecuteState ies = (MappingExecuteState)((ExecutableContainer)iac).getExecuteState();
	
				ies.setExecuteState(MappingState.EXECUTING);
				ies.setExecuteStartedAt(executeStart);
				ies.setExecuteMessage(null);
				ies.setExecuteShards(0);
				ies.setCount(0);
				ies.clearMessages();
				ies.setExecuteMessage(null);
			});
		} catch (Exception ex) {
			throw new TaskFailureException(ex, new Date());
		}		

		logger.info(oc.getClass().getName() + oc.getPrimaryId() + " starting");
		
		em.sendMessage(new ExecuteNotificationObject(oc));
		
		try (FileSystemRDFOutputHandler outhandler = folderService.createExecutionRDFOutputHandler(oc, shardSize)) {

			ExecutionOptions eo = oc.buildExecutionParameters();

			String str = oc.applyPreprocessToMappingDocument(eo);

			Executor exec = new Executor(outhandler, safeExecute);
			
			folderService.checkCreateExtractTempFolder(oc.getCurrentUser());

			try  {
				exec.setMonitor(em);

				D2RMLModel d2rml = D2RMLModel.readFromString(str);
				
				exec.configureFileExtraction(extractMinSize, folderService.getExtractTempFolder(oc.getCurrentUser()), d2rml.usesCaches() ? restCacheSize : 0);
				
				em.createStructure(d2rml, outhandler);

				logger.info(oc.getClass().getName() + " started -- id: " + oc.getPrimaryId());

				em.sendMessage(new ExecuteNotificationObject(oc));
				
//				exec.keepSubjects(true);
				exec.execute(d2rml, eo.getParams());

//				Set<Resource> subjects = exec.getSubjects();
//				writeExecutionCatalog(exec.getSubjects(), oc);
				
				em.complete();
				
				oc.update(iac -> {			    
					MappingExecuteState ies = (MappingExecuteState)((ExecutableContainer)iac).getExecuteState();

					ies.setExecuteState(MappingState.EXECUTED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(outhandler.getShards());
					ies.setCount(outhandler.getTotalItems());
//					ies.setCount(subjects.size());
					
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getContent().getProgress());
					ies.setExecuteMessage(null);
				});

//				em.sendMessage(new ExecuteNotificationObject(oc), subjects.size());
				em.sendMessage(new ExecuteNotificationObject(oc), outhandler.getTotalItems());

				logger.info(oc.getClass().getName() + " executed -- id: " + oc.getPrimaryId() + ", shards: " + outhandler.getShards());

				if (outhandler.getTotalItems() > 0) {
					try {
						zipExecution(oc, outhandler.getShards());
					} catch (Exception ex) {
						ex.printStackTrace();
						
						logger.info("Zipping " + oc.getClass().getName() + " execution failed -- id: " + oc.getPrimaryId());
					}
				}
				
				return new AsyncResult<>(em.getCompletedAt());

			} catch (Exception ex) {
				logger.info(oc.getClass().getName() + " failed -- id: " + oc.getPrimaryId());
				
				em.currentConfigurationFailed(ex);

				throw ex;
			} finally {
				exec.finalizeFileExtraction();
				
				try {
					if (em != null) {
						em.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			em.complete(ex);

			try {
				oc.update(iac -> {			    
					MappingExecuteState ies = (MappingExecuteState)((ExecutableContainer)iac).getExecuteState();
	
					ies.setExecuteState(MappingState.EXECUTION_FAILED);
					ies.setExecuteCompletedAt(em.getCompletedAt());
					ies.setExecuteShards(0);
					ies.setCount(0);
					ies.setMessage(em.getFailureMessage());
					ies.setD2rmlExecution(((ExecuteNotificationObject)em.lastSentNotification()).getContent().getProgress());
					ies.setExecuteMessage(null);
				});
			} catch (Exception iex) {
				throw new TaskFailureException(iex, em.getCompletedAt());
			}
			
			em.sendMessage(new ExecuteNotificationObject(oc));
			
			throw new TaskFailureException(ex, em.getCompletedAt());
		}

	}

	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("publish " + tdescr.getType());
		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();

		PublishableContainer<?,?,ExecuteState,PublishState<ExecuteState>,?> pc = (PublishableContainer)tdescr.getContainer();
		
		try {
			pc.update(ioc -> {
				PublishState<?> ips = ((PublishableContainer<?,?,?,?,?>)ioc).getPublishState();
				ips.startDo(pm);
			});
	
			pm.sendMessage(new PublishNotificationObject(pc));
	
			logger.info("Publication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " started.");
			
			pc.publish(tdescr.getProperties());

			pm.complete();

			pc.update(ioc -> {
				PublishState<ExecuteState> ips = (PublishState)((PublishableContainer)ioc).getPublishState();
				ips.completeDo(pm);
				ips.setExecute(((ExecutableContainer<?,?,?,Dataset>)ioc).getExecuteState());
			});

			logger.info("Publication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " completed.");

			pm.sendMessage(new PublishNotificationObject(pc));
		
			System.out.println("publish X1");
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);

			System.out.println("publish X2");
			
			try {
				pc.update(ioc -> {
					PublishState<ExecuteState> ips = ((PublishableContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm);
					}
				});
				
				System.out.println("publish X3");
				
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

		PublishableContainer<?,?,ExecuteState,PublishState<ExecuteState>,Dataset> pc = (PublishableContainer)tdescr.getContainer();
		
		try {
			pc.update(ioc -> {
				PublishState<ExecuteState> ips = ((PublishableContainer)ioc).getPublishState();
				ips.startUndo(pm);
			});
	
			pm.sendMessage(new PublishNotificationObject(pc));
			
			logger.info("Unpublication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " started.");
			
			pc.unpublish(tdescr.getProperties());

			pm.complete();
			
			pc.update(ioc -> {
				PublishableContainer<?,?,ExecuteState, PublishState<ExecuteState>,Dataset> ipc = (PublishableContainer)ioc;
				ExecutableContainer<?,?,ExecuteState,Dataset> iec = (ExecutableContainer)ioc;
				
				PublishState<ExecuteState> ips = ipc.getPublishState();
				
				ipc.removePublishState(ips);
				
				ExecuteState ies = iec.getExecuteState();
				ExecuteState ipes = ips.getExecute();
				if (ies != null && ipes != null && ies.getExecuteStartedAt().compareTo(ipes.getExecuteStartedAt()) != 0) {
					iec.clearExecution(ipes);
				}
			});

			logger.info("Unpublication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(pc));
		
			return new AsyncResult<>(pm.getCompletedAt());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);

			try {
				pc.update(ioc -> {
					PublishState<ExecuteState> ips = ((PublishableContainer)ioc).checkPublishState();
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

	public Date preRun(TaskDescription tdescr, WebSocketService wsService) {
 		TaskMonitor tm = tdescr.getMonitor();
 		RunnableContainer<?,?> oc = (RunnableContainer<?,?>)tdescr.getContainer();
 		
		if (tm != null) {
			try {
				oc.update(iec -> {			
					RunState ies = ((RunnableContainer<?,?>)iec).getRunState(); 
					ies.startDo(tm);
				});
			
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
			NotificationType type = tdescr.getTaskSpecification().getNotificationType();
	
			NotificationObject no = NotificationObject.createNotificationObject(type, RunningState.RUNNING.toString(), tdescr.getContainer());
	
			if (no != null) {
				tm.sendMessage(no);
			}
		}
		
		return new Date();
	}
	
	public Date postRunSuccess(TaskDescription tdescr, WebSocketService wsService) {
 		TaskMonitor tm = tdescr.getMonitor();
 		RunnableContainer<?,?> oc = (RunnableContainer<?,?>)tdescr.getContainer();
 		
		if (tm != null) {
			tm.complete();
			
			try {
				oc.update(iec -> {			
					RunState ies = ((RunnableContainer<?,?>)iec).getRunState(); 
					ies.completeDo(tm);
				});
			
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
			NotificationType type = tdescr.getTaskSpecification().getNotificationType();
	
			NotificationObject no = NotificationObject.createNotificationObject(type, RunningState.RUN.toString(), tdescr.getContainer());
			if (no != null) {
				tdescr.getMonitor().sendMessage(no);
			}
			
			return tm.getCompletedAt();
			
		} else {
			return new Date();
		}
	}
	
	public Date postRunFail(TaskDescription tdescr, WebSocketService wsService) {
 		TaskMonitor tm = tdescr.getMonitor();
 		RunnableContainer<?,?> oc = (RunnableContainer<?,?>)tdescr.getContainer();
 		
		if (tm != null) {
			tm.complete();
			
			try {
				oc.update(iec -> {			
					RunState ies = ((RunnableContainer<?,?>)iec).getRunState(); 
					ies.failDo(tm);
				});
			
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
			NotificationType type = tdescr.getTaskSpecification().getNotificationType();

			NotificationObject no = NotificationObject.createNotificationObject(type, RunningState.RUNNING_FAILED.toString(), tdescr.getContainer());
			if (no != null) {
				tm.sendMessage(no);
			}
			return tm.getCompletedAt();
			
		} else {
			return new Date();
		}
	}
	
	@Async("shaclValidationExecutor")
	public ListenableFuture<Date> validate(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor gm = (GenericMonitor)tdescr.getMonitor();

		ValidatableContainer<?,?> vc = (ValidatableContainer<?,?>)tdescr.getContainer();

		try {

			vc.update(ioc -> {
				ValidateState ivs = ((ValidatableContainer<?,?>)ioc).getValidateState();
				ivs.startDo(gm);
				ivs.setValidatorDocumentId(vc.getValidatorDocument().stream().map(e -> e.getId()).collect(Collectors.toList()));
			});
			
			gm.sendMessage(new ValidateNotificationObject(vc));
			
			logger.info("Validating of " + vc.getClass().getName() + ":"  + vc.getPrimaryId() + " started.");
			
			ValidationResult vr = vc.validate();
			
			gm.complete();
			
		    vc.update(ioc -> {
		    	ValidateState ivs = ((ValidatableContainer<?,?>)ioc).getValidateState();
				ivs.completeDo(gm);
				ivs.setResult(vr);
			});

			logger.info("Validating of " + vc.getClass().getName() + ":"  + vc.getPrimaryId() + " completed.");

			gm.sendMessage(new ValidateNotificationObject(vc));

			return new AsyncResult<>(gm.getCompletedAt());
		    
		} catch (Exception ex) {
			logger.info("Validating of " + vc.getClass().getName() + ":"  + vc.getPrimaryId() + " failed.");
			
			ex.printStackTrace();
			
			gm.complete(ex);
			
			try {
				vc.update(ioc -> {
					ValidateState ivs = ((ValidatableContainer<?,?>)ioc).checkValidateState();
					if (ivs != null) {
						ivs.failDo(gm);
					}
				});
				
				if (vc.checkValidateState() != null) {
					gm.sendMessage(new ValidateNotificationObject(vc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, gm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, gm.getCompletedAt());			
			
		} finally {
			try {
				if (gm != null) {
					gm.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	public <D extends SpecificationDocument, F extends Response> boolean clearExecution(ExecutableContainer<D,F,?,Dataset> oc) throws Exception {
		MappingExecuteState es = (MappingExecuteState)((ExecutableContainer<?,?,?,Dataset>)oc).checkExecuteState();
		
		if (es == null || es.getExecuteState() != MappingState.EXECUTED) {
			return false;
		}
		
		return clearExecution(oc, es);
	}
	
	public <D extends SpecificationDocument, F extends Response> boolean clearExecution(ExecutableContainer<D,F,?,Dataset> oc, MappingExecuteState es) throws Exception {
		
		PublishableContainer<?,F,ExecuteState, PublishState<ExecuteState>,Dataset> pc = (PublishableContainer)oc;
		ExecutableContainer<?,F,ExecuteState,Dataset> ec = (ExecutableContainer)oc;
		
		ProcessStateContainer psv = pc.getCurrentPublishState();
		
		if (psv != null) {
			PublishState<ExecuteState> ps = (PublishState)psv.getProcessState();
			ExecuteState pes = ps.getExecute();
	
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
			FileUtils.deleteFile(folderService.getExecutionCatalogFile(ec, es));
		} catch (Exception ex) {
//			ex.printStackTrace();
		}
		
		try {
			FileUtils.deleteFile(folderService.getExecutionFile(ec, es, FileType.zip));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		try {
			FileUtils.deleteFile(folderService.getExecutionFile(ec, es, FileType.xlsx));
		} catch (Exception ex) {
//			ex.printStackTrace();
		}
		
		try {
			FileUtils.deleteFile(folderService.getExecutionFile(ec, es, FileType.txt));
		} catch (Exception ex) {
//			ex.printStackTrace();
		}
		
		folderService.deleteContainingFolderIfEmpty(oc);
		
		if (ec.checkExecuteState() == es) {
			oc.update(ioc -> {
				PublishableContainer<?,F,ExecuteState, PublishState<ExecuteState>,Dataset> ipc = (PublishableContainer)ioc;
				ExecutableContainer<?,F,MappingExecuteState,Dataset> iec = (ExecutableContainer)ioc;
				
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
						ines.setExecuteMessage(ipes.getExecuteMessage());
					}
				}
			});
		} 
		
		return true;
	}

	
	public File zipExecution(ExecutableContainer<?,?,?,Dataset> ec, int shards) throws IOException {
		return zipExecution(ec, ec.getExecuteState(), shards);
	}	
	
	public File zipExecution(ExecutableContainer<?,?,?,Dataset> ec, ExecuteState es, int shards) throws IOException {
		
		File file = folderService.createExecutionFile(ec, es, FileType.zip);
		
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

	
	public HttpClient getSSHClient() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		SSLContext sslContext = new SSLContextBuilder()
			      .loadTrustMaterial(null, new TrustStrategy() {
					@Override
					public boolean isTrusted(X509Certificate[] certificate, String authType)
							throws CertificateException {
						return true;
					}
				}).build();
			 
		HttpClient client = HttpClients.custom()
			      .setSSLContext(sslContext)
			      .setSSLHostnameVerifier(new NoopHostnameVerifier())
			      .build();
		
		return client;
	}
	
	public org.apache.jena.query.Dataset load(ExecutableContainer<?,?,?,Dataset> ec) throws Exception {
		return ((JenaAccessWrapper)load(ec, RDFLibrary.JENA)).getDataset();
		
	}
	public RDFAccessWrapper load(ExecutableContainer<?,?,?,Dataset> ec, RDFLibrary rdfLibrary) throws Exception {
		
		RDFAccessWrapper ds = RDFAccessWrapper.create(rdfLibrary);

		ExecuteState es = ec.getExecuteDocument().checkExecuteState(fileSystemConfiguration.getId());
			
		if (es instanceof MappingExecuteState) {
			MappingExecuteState mes = (MappingExecuteState)es;
			
			if (mes.getExecuteShards() != null) {
				for (int i = 0; i < mes.getExecuteShards(); i++) {
					File f = folderService.getExecutionTrigFile(ec, mes, i);
					if (f != null) {
						ds.load(f);
					}
				}
			}
		} else if (es instanceof FileExecuteState) {
			// ?? 
		}
		
		return ds;
	}
	
	
//	try (RepositoryConnection con = paramDataset.getConnection()) {
//		executionResultsToModel(con, currentUser, dependecyMappingIds.get(db.getName()));
//	}
	
	public Graph loadAsGraph(ExecutableContainer<?,?,?,Dataset> ec) throws IOException {

		Graph ds = GraphFactory.createDefaultGraph();

		ExecuteState es = ec.getExecuteDocument().checkExecuteState(fileSystemConfiguration.getId());
		
		if (es instanceof MappingExecuteState) {
			MappingExecuteState mes = (MappingExecuteState)es;
			
			if (mes.getExecuteShards() != null) {
				for (int i = 0; i < mes.getExecuteShards(); i++) {
					File f = folderService.getExecutionTrigFile(ec, mes, i);
					if (f != null) {
						RDFDataMgr.read(ds, "file:" + f.getCanonicalPath(), null, Lang.TRIG);
					}
				}
			}
		} else if (es instanceof FileExecuteState) {
			// ?? 
		}
		
		return ds;
	}

	@Async
	public ListenableFuture<Date> dummyTask(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		return new AsyncResult<>(new Date());
	}
	
	public String syncString(String id, Class<?> container) {
		return ("SYNC:" + database.getName() + ":" + container.getName() + ":" + id).intern();
	}
	
	
	public void writeExecutionCatalog(Set<Resource> subjects, SideExecutableContainer<?,?,?,Dataset> ec) throws IOException {
		if (subjects.size() > 0) {
			try (Writer sw = new OutputStreamWriter(new FileOutputStream(folderService.createExecutionCatalogFile(ec, ec.getExecuteState()), false), StandardCharsets.UTF_8)) {
				
				sw.write("<" + resourceVocabulary.getAnnotationSetAsResource(ec.getObject().getUuid()).toString() + ">\n");
				sw.write("        <http://purl.org/dc/terms/hasPart>\n");
				sw.write("                ");
				int c = 0;
				for (Resource r : subjects) {
					if (c++ > 0) {
						sw.write(" , ");
					}
					sw.write("<" + r.getURI() + ">");
				}
				sw.write(" .");
			}
		}
	}
	
	private int BUFFER_SIZE = 2048;

	public MultipartFile download(String url) throws Exception {
		HttpGet request = new HttpGet(url);
		
		URL urlx = new URL(url);

		SSLContext sslContext = new SSLContextBuilder()
			      .loadTrustMaterial(null, new TrustStrategy() {
					@Override
					public boolean isTrusted(X509Certificate[] certificate, String authType)
							throws CertificateException {
						return true;
					}
				}).build();
			
			HttpClientBuilder clientBuilder = HttpClients.custom();
			
			clientBuilder = clientBuilder
			//.setRedirectStrategy(new LaxRedirectStrategy())
		      .setSSLContext(sslContext)
		      .setSSLHostnameVerifier(new NoopHostnameVerifier());

				 
			CloseableHttpClient client = clientBuilder.build();
			
			HttpResponse response = client.execute(request);
			
			
	        byte buffer[] = new byte[BUFFER_SIZE];
			int count;

            try (ByteArrayOutputStream fos = new ByteArrayOutputStream(); 
            		BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER_SIZE)) {
                while ((count = response.getEntity().getContent().read(buffer, 0, BUFFER_SIZE)) != -1) {
                    dest.write(buffer, 0, count);
                }
                dest.flush();


                return new CustomMultipartFile(fos.toByteArray(), FilenameUtils.getName(urlx.getPath()));
            }

	}
	
	private class CustomMultipartFile implements MultipartFile {
	    private byte[] input;
	    private String fileName;

	    public CustomMultipartFile(byte[] input, String fileName) {
	        this.input = input;
	        this.fileName = fileName; 
	    }

	    @Override
	    public String getName() {
	        return null;
	    }

	    @Override
	    public String getOriginalFilename() {
	        return fileName;
	    }

	    @Override
	    public String getContentType() {
	        return null;
	    }

	    @Override
	    public boolean isEmpty() {
	        return input == null || input.length == 0;
	    }

	    @Override
	    public long getSize() {
	        return input.length;
	    }

	    @Override
	    public byte[] getBytes() throws IOException {
	        return input;
	    }

	    @Override
	    public InputStream getInputStream() throws IOException {
	        return new ByteArrayInputStream(input);
	    }

	    @Override
	    public void transferTo(File destination) throws IOException, IllegalStateException {
	        try(FileOutputStream fos = new FileOutputStream(destination)) {
	            fos.write(input);
	        }
	    }

	}

}
