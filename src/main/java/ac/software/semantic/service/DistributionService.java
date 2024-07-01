package ac.software.semantic.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.state.CreateState;
import ac.software.semantic.model.state.IndexState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.base.CreatableDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.SerializationType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.SchemaService.ClassStructure;
import ac.software.semantic.service.container.CreatableContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.EnclosedContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.monitor.GenericMonitor;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoConstructIterator;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import ac.software.semantic.payload.notification.CreateNotificationObject;
import ac.software.semantic.payload.request.DistributionUpdateRequest;
import ac.software.semantic.payload.response.DistributionDocumentResponse;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.DistributionDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;

@Service
public class DistributionService implements EnclosedCreatableService<DistributionDocument, DistributionDocumentResponse, DistributionUpdateRequest, Dataset>,
                                            IdentifiableDocumentService<DistributionDocument, DistributionDocumentResponse> {

	private Logger logger = LoggerFactory.getLogger(DistributionService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private FolderService folderService;

	@Autowired
	@Qualifier("triplestore-configurations")
	private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

 	@Autowired
	@Qualifier("filesystem-configuration")
	private FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	private DistributionDocumentRepository distributionDocumentRepository;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private TaskRepository taskRepository;
	
	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends EnclosedObjectContainer<DistributionDocument,DistributionDocumentResponse,Dataset>> getContainerClass() {
		return DistributionContainer.class;
	}
	
	@Override
	public DocumentRepository<DistributionDocument> getRepository() {
		return distributionDocumentRepository;
	}

	
	public class DistributionContainer extends EnclosedObjectContainer<DistributionDocument, DistributionDocumentResponse,Dataset> 
	                            implements UpdatableContainer<DistributionDocument, DistributionDocumentResponse,DistributionUpdateRequest>, 
	                                       CreatableContainer<DistributionDocument, DistributionDocumentResponse,IndexState,Dataset>,
	                                       EnclosedContainer<DistributionDocument, Dataset> {
		private ObjectId indexId;
		
		private DistributionContainer(UserPrincipal currentUser, ObjectId indexId) {
			this.currentUser = currentUser;
			
			this.indexId = indexId;
		
			load();
		}
		
		private DistributionContainer(UserPrincipal currentUser, DistributionDocument idoc) {
			this(currentUser, idoc, null);
		}
		
		private DistributionContainer(UserPrincipal currentUser, DistributionDocument idoc, Dataset dataset) {
			this.currentUser = currentUser;

			this.indexId = idoc.getId();
			this.object = idoc;
			
			this.dataset = dataset;
		}

		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return indexId;
		}
		
		@Override
		public DocumentRepository<DistributionDocument> getRepository() {
			return distributionDocumentRepository;
		}
		
		@Override
		public DistributionService getService() {
			return DistributionService.this;
		}
		
		@Override
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}

		@Override
		protected void loadDataset() {
			Optional<Dataset> datasetOpt = datasetRepository.findByUuid(object.getDatasetUuid());

			if (!datasetOpt.isPresent()) {
				return;
			}
		
			setEnclosingObject(datasetOpt.get());
		}
		
		@Override
		public CreatableDocument<IndexState> getCreateDocument() {
			return getObject();
		}
		
//		@Override 
//		public Dataset getEnclosingObject() {
//			return getDataset();
//		}
		
		@Override
		public DistributionDocument update(DistributionUpdateRequest ur) throws Exception {

			return update(idc -> {
				DistributionDocument dd = idc.getObject();
				dd.setName(ur.getName());
				dd.setIdentifier(ur.getIdentifier());
				dd.setCompress(ur.getCompress());
				dd.setLicense(ur.getLicense());
				dd.setClasses(ur.getClasses());
				dd.setSerializations(ur.getSerializations());
				dd.setSerializationVocabulary(ur.getSerializationVocabulary());
			});
		}
		
		@Override
		public boolean delete() throws Exception {
			synchronized (saveSyncString()) {
					
				distributionDocumentRepository.delete(object);
	
				return true;
			}
		}

		@Override
		public String localSynchronizationString() {
			return getObject().getId().toString();
		}

		@Override
		public DistributionDocumentResponse asResponse() {
	    	DistributionDocumentResponse response = new DistributionDocumentResponse();
	    	response.setId(object.getId().toString());
	    	response.setName(object.getName());
	    	response.setIdentifier(object.getIdentifier());
	    	response.setClasses(object.getClasses());
	    	response.setCompress(object.getCompress());
	    	response.setLicense(object.getLicense());
	    	response.setSerializations(object.getSerializations());
	    	response.setSerializationVocabulary(object.getSerializationVocabulary());
	    	response.setCreatedAt(object.getCreatedAt());
	    	response.setUpdatedAt(object.getUpdatedAt());
	    	
	    	response.copyStates(object, null, fileSystemConfiguration);

	    	return response;
		}
		
		@Override
		public String getDescription() {
			return getObject().getName();
		}

		@Override
		public TaskType getCreateTask() {
			return TaskType.DISTRIBUTION_CREATE;
		}

		@Override
		public TaskType getDestroyTask() {
			return TaskType.DISTRIBUTION_DESTROY;
		}

		@Override
		public TaskType getRecreateTask() {
			return TaskType.DISTRIBUTION_RECREATE;
		}
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			return taskRepository.findActiveByDistributionId(getObject().getId(), type).orElse(null);
		}

		@Override
		public FileSystemConfiguration getContainerFileSystemConfiguration() {
			return fileSystemConfiguration;
		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}
	
	@Override
	public DistributionContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		DistributionContainer ec = new DistributionContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
	
	@Override
	public DistributionContainer getContainer(UserPrincipal currentUser, DistributionDocument idoc, Dataset dataset) {
		DistributionContainer ec = new DistributionContainer(currentUser, idoc, dataset);

		if (ec.getObject() == null || ec.getEnclosingObject() == null) {
			return null;
		} else {
			return ec;
		}
	}
    
	@Override
	public DistributionDocument create(UserPrincipal currentUser, Dataset dataset, DistributionUpdateRequest ur) throws Exception {
		
		DistributionDocument ddoc = new DistributionDocument(dataset);
		ddoc.setUserId(new ObjectId(currentUser.getId()));
		
		ddoc.setName(ur.getName());
		ddoc.setIdentifier(ur.getIdentifier());
		ddoc.setClasses(ur.getClasses());
		ddoc.setCompress(ur.getCompress());
		ddoc.setLicense(ur.getLicense());
		ddoc.setSerializations(ur.getSerializations());
		ddoc.setSerializationVocabulary(ur.getSerializationVocabulary());
		
		return create(ddoc);
	}

	@Async("createDistributionExecutor")
	public ListenableFuture<Date> create(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		DistributionContainer dc = (DistributionContainer)tdescr.getContainer();
    	DistributionDocument distr = dc.getObject();
    	
		try {
	    	Dataset dataset = dc.getEnclosingObject();
			TripleStoreConfiguration vc = dataset.getPublishVirtuosoConfiguration(virtuosoConfigurations.values());
			
			DatasetCatalog dcg = schemaService.asCatalog(dataset.getUuid());
			String fromClause = schemaService.buildFromClause(dcg);

			logger.info("Creating distribution " + distr.getUuid());

			pm.sendMessage(new CreateNotificationObject(dc), "Clearing previous distribution");

			Thread.sleep(1000);

			clearDistribution(dc);

//			pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Clearing relevant metadata")));
			pm.sendMessage(new CreateNotificationObject(dc), "Clearing distribution metadata");
			
			Thread.sleep(1000);

//			tripleStore.clearDistributionToMetadata(dc);

			dc.update(idc -> {
				IndexState ics = ((CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset>)idc).getCreateState();
				ics.startDo(pm);
			});
			
//	    	pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Reading top classes")));
			pm.sendMessage(new CreateNotificationObject(dc), "Reading top classes");	    	
			
	    	Thread.sleep(1000);
	    	
	    	Map<String, ClassStructure> structMap = new HashMap<>();
	    	for (ClassStructure cs : schemaService.readTopClasses(dc.getEnclosingObject())) {
	    		structMap.put(cs.getClazz().toString(), cs);
	    	}
	    	
	    	List<ClassStructure> structs = new ArrayList<>();
	    	for (String clazz : distr.getClasses()) {
	    		ClassStructure cs = structMap.get(clazz);
	    		if (cs != null) {
	    			structs.add(cs);
	    		}
	    	}
	    	
			File ttlFile = null;
			File ntFile = null;
			FileWriter ttlWriter = null;
			FileWriter ntWriter = null;
	    	if (distr.getSerializations().contains(SerializationType.TTL)) {
	    		ttlFile = folderService.createDatasetDistributionFile(dc.getCurrentUser(), dataset, distr, dc.checkCreateState(), SerializationType.TTL);
	    		ttlWriter = new FileWriter(ttlFile);
	    	}
	    	
	    	if (distr.getSerializations().contains(SerializationType.NT)) {
	    		ntFile = folderService.createDatasetDistributionFile(dc.getCurrentUser(), dataset, distr, dc.checkCreateState(), SerializationType.NT);
	    		ntWriter = new FileWriter(ntFile);
	    	}
	    	
			try {
	
				int csCount = 0;
		    	for (ClassStructure cs : structs) {
//		    		logger.info("Exporting " + cs.getClazz());

//		    		pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Reading " + cs.getClazz() + " instances")));
					pm.sendMessage(new CreateNotificationObject(dc), "Reading " + cs.getClazz() + " instances");
		    		
		    		Thread.sleep(1000);
		    		
					StringBuffer db = new StringBuffer();
					
					db.append("CONSTRUCT { ?resource ?p1 ?o1 . ");
					for (int j = 2; j <= cs.getDepth(); j++) {
						db.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
					}
					db.append("} " + fromClause + " WHERE { ?resource  ?p1 ?o1 . ");
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
					}			
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db.append(" } ");
					}
		    		db.append("VALUES ?resource { {@@RESOURCE@@} }");
					db.append("}");
					
					StringBuffer db2 = new StringBuffer();
					
					db2.append("CONSTRUCT { {@@RESOURCE@@} ?p1 ?o1 . ");
					for (int j = 2; j <= cs.getDepth(); j++) {
						db2.append(" ?o" + (j-1) + " ?p" + j + " ?o" + j + ". ");
					}
					db2.append("} WHERE { {@@RESOURCE@@}  ?p1 ?o1 . ");
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db2.append("OPTIONAL { ?o" + (j-1) + " ?p" + j + " ?o" + j + " . FILTER(isBlank(?o" + (j-1) + ")) ");
					}			
		    		for (int j = 2; j <= cs.getDepth(); j++) {
						db2.append(" } ");
					}
					db2.append("}");
					
					String itemsSparql = db.toString();
					String resourceSparql = db2.toString();

					String countSparql =
							" SELECT (COUNT(?item) AS ?count) " + fromClause + " WHERE { " +
							"    ?item a <" + cs.getClazz() + "> . " + 
							" } ";
					
					int totalCount = 0;
					try (VirtuosoSelectIterator qe = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), countSparql)) {
						while (qe.hasNext()) {
							QuerySolution sol = qe.next();
							
							totalCount = sol.get("count").asLiteral().getInt();
						}
					}
					
					String sparql =
							"SELECT ?item " + fromClause + " WHERE { " + 
							" SELECT ?item WHERE { " +
							"    ?item a <" + cs.getClazz() + "> . " + 
							" } ORDER BY ?item }";
					
	
					if (ttlWriter != null && csCount > 0) {
						ttlWriter.append("\n");
					}
					
					csCount++;
	
					int count = 0;
					
//					System.out.println(QueryFactory.create(sparql));
					
					int pageSize = Math.min(1000, VirtuosoSelectIterator.VIRTUOSO_LIMIT / (cs.getSize() > 0 ? cs.getSize() : 1));
					
//					System.out.println("page size " + pageSize);
					
					try (VirtuosoSelectIterator qe = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {

//						pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Exporting " + cs.getClazz() + " (" + count + "/" + totalCount + ")")));
						pm.sendMessage(new CreateNotificationObject(dc), "Exporting " + cs.getClazz() + " (" + count + "/" + totalCount + ")");
						
						Thread.sleep(1000);

						List<Resource> buffer = new ArrayList<>();
//						int buffCount = 0;
						
						while (qe.hasNext()) {
							if (Thread.currentThread().isInterrupted()) {
								Exception ex = new InterruptedException("The task was interrupted.");
								throw ex;
							}
							
							QuerySolution sol = qe.next();
							
							Resource item = sol.getResource("item");
		
							if (buffer.size() < pageSize) {
								buffer.add(item);
							} else {
//								pm.sendMessage(new CreateDistributionNotificationObject(cds.getCreateDistributionState(), dc, new NotificationMessage(MessageType.INFO, "Exporting " + cs.getClazz() + " (" + count + "/" + totalCount + ")")));
								pm.sendMessage(new CreateNotificationObject(dc), "Exporting " + cs.getClazz() + " (" + count + "/" + totalCount + ")");
								
								Thread.sleep(1000);

//								System.out.println(buffCount++ + " " + count);
								
								StringBuffer stringBuffer = new StringBuffer();
								for (Resource r : buffer) {
									stringBuffer.append("<" + r.toString() + ">" );
								}
								
								String iSparql = itemsSparql.replaceAll("\\{@@RESOURCE@@\\}", stringBuffer.toString());
		
//								if (ttlWriter != null && count > 0) {
//									ttlWriter.append("\n");
//								}
								
								try (QueryExecution iqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), iSparql)) {
									
									Model model = VirtuosoConstructIterator.tryQuery(iqe, iSparql, logger);
//									model.clearNsPrefixMap();
									
									for (Resource r : buffer) {
										String iiSparql = resourceSparql.replaceAll("\\{@@RESOURCE@@\\}", "<" + r.toString() + ">");
										
										try (QueryExecution iiqe = QueryExecutionFactory.create(iiSparql, model)) {
											
											Model imodel = iiqe.execConstruct();
											imodel.clearNsPrefixMap();
											
											if (ttlWriter != null && count > 0) {
												ttlWriter.append("\n");
											}
											
											if (ttlWriter != null) {
												RDFDataMgr.write(ttlWriter, imodel, Lang.TURTLE);
											}
											
											if (ntWriter != null) {
												RDFDataMgr.write(ntWriter, imodel, Lang.NT);
											}
											
											count++;

										}
									}
								}
								
								buffer = new ArrayList<>();
								buffer.add(item);
							}
						}
						
						if (buffer.size() > 0) {
//							System.out.println(buffCount++ + " " + count);
							
							StringBuffer stringBuffer = new StringBuffer();
							for (Resource r : buffer) {
								stringBuffer.append("<" + r.toString() + ">" );
							}
							
							String iSparql = itemsSparql.replaceAll("\\{@@RESOURCE@@\\}", stringBuffer.toString());
	
//							if (ttlWriter != null && count > 0) {
//								ttlWriter.append("\n");
//							}
							
							try (QueryExecution iqe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), iSparql)) {
								
								Model model = VirtuosoConstructIterator.tryQuery(iqe, iSparql, logger);
//								model.clearNsPrefixMap();

								for (Resource r : buffer) {
									String iiSparql = resourceSparql.replaceAll("\\{@@RESOURCE@@\\}", "<" + r.toString() + ">");
									
									try (QueryExecution iiqe = QueryExecutionFactory.create(iiSparql, model)) {
										
										Model imodel = iiqe.execConstruct();
										imodel.clearNsPrefixMap();
										
										if (ttlWriter != null && count > 0) {
											ttlWriter.append("\n");
										}
										
										if (ttlWriter != null) {
											RDFDataMgr.write(ttlWriter, imodel, Lang.TURTLE);
										}
										
										if (ntWriter != null) {
											RDFDataMgr.write(ntWriter, imodel, Lang.NT);
										}
										
										count++;
									}
								}
							}
						}
					}
					
//					System.out.println(cs.getClazz() + " : " + count);
					logger.info("Exported " + count + " items for " + cs.getClazz());
				}
		    	
		    	
		    	if (ttlWriter != null) {
		    		ttlWriter.flush();
		    	}
		    	
		    	if (ntWriter != null) {
		    		ntWriter.flush();
		    	}
	    	} finally {
	    		if (ttlWriter != null) {
	    			ttlWriter.close();
	    		}

	    		if (ntWriter != null) {
	    			ntWriter.close();
	    		}
	    	}
		    
			if (distr.getCompress().equals("ZIP")) {
				if (ttlFile != null) {
					zipDistribution(dc, SerializationType.TTL, ttlFile);
				}

				if (ntFile != null) {
					zipDistribution(dc, SerializationType.NT, ntFile);
				}

			}

//	    	tripleStore.addDistributionToMetadata(dc);

	    	pm.complete();
	    	
			dc.update(ioc -> {
				CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset> coc = (CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset>)ioc;
				
				IndexState iis = coc.getCreateState();
				iis.completeDo(pm);
				iis.setPublish((PublishState<?>)(coc.getEnclosingObject().getCurrentPublishState(virtuosoConfigurations.values()).getProcessState()));
			});

			logger.info("Distribution " + distr.getUuid() + " created");
			
			pm.sendMessage(new CreateNotificationObject(dc));

			return new AsyncResult<>(pm.getCompletedAt());

		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
			try {
				dc.update(ioc -> {
					CreateState ics = ((CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset>)ioc).checkCreateState();
					if (ics != null) {
						ics.failDo(pm);
					}
				});
				
				if (dc.checkCreateState() != null) {
					pm.sendMessage(new CreateNotificationObject(dc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
	    	throw new TaskFailureException(ex, pm.getCompletedAt());
		}
    }
	
	@Async("createDistributionExecutor")
	public ListenableFuture<Date> destroy(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
		
		DistributionContainer dc = (DistributionContainer)tdescr.getContainer();
		
		try {
			CreateState prevcs = dc.checkCreateState(); // will be changed so keep to for file name generation
			
			dc.update(idc -> {
				CreateState iis = ((CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset>)idc).getCreateState();
				iis.startUndo(pm);
			});
			
			pm.sendMessage(new CreateNotificationObject(dc));
			
			if (prevcs != null) {
				logger.info("Destroying distribution " + dc.getPrimaryId() + " started.");
	
				clearDistribution(dc.getCurrentUser(), dc.getEnclosingObject(), dc.getObject(), prevcs);
			}
			
//			tripleStore.clearDistributionToMetadata(dc);
			
			pm.complete();
			
			dc.update(idc -> {
				((CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset>)idc).deleteCreateState();
			});
			
			logger.info("Destroying distribution " + dc.getPrimaryId() + " completed.");
			
			pm.sendMessage(new CreateNotificationObject(dc));
			
			return new AsyncResult<>(pm.getCompletedAt());
		
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);
			
			try {
				dc.update(ioc -> {
					CreateState iis = ((CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset>)ioc).checkCreateState();
					if (iis != null) {
						iis.failUndo(pm);
					}
				});
				
				if (dc.checkCreateState() != null) {
					pm.sendMessage(new CreateNotificationObject(dc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}	
	
	private File zipDistribution(DistributionContainer dc, SerializationType serialization, File fileToZip) throws IOException {
		
		File file = folderService.createDatasetDistributionZipFile(dc.getCurrentUser(), dc.getEnclosingObject(), dc.getObject(), dc.checkCreateState(), serialization);
		
		try (FileOutputStream fos = new FileOutputStream(file);
			ZipOutputStream zipOut = new ZipOutputStream(fos)) {
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
    	
		return file;
	}
	
	public boolean clearDistribution(DistributionContainer dc) throws Exception {

		DistributionDocument distr = dc.getObject();
				
		CreateState cs = dc.checkCreateState();
		
		if (cs != null) {
			clearDistribution(dc.getCurrentUser(), dc.getEnclosingObject(), distr, cs);

			dc.update(idc -> {
				((CreatableContainer<DistributionDocument,DistributionDocumentResponse,IndexState,Dataset>)idc).deleteCreateState();
			});

			return true;
		} else {
			return false;
		}
	}

	public boolean clearDistribution(UserPrincipal currentUser, Dataset dataset, DistributionDocument doc, CreateState cds) {
		
		for (SerializationType st : doc.getSerializations()) {
			try {
				File f = folderService.getDatasetDistributionFile(currentUser, dataset, doc, cds, st);
				boolean ok = false;
				if (f != null) {
					ok = f.delete();
					if (ok) {
						logger.info("Deleted file " + f.getAbsolutePath());
					}
					
				}
				if (!ok) {
					logger.warn("Failed to delete distribution " + doc.getIdentifier() + " for dataset " + dataset.getUuid());
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}			
	
			try {
				File f = folderService.getDatasetDistributionZipFile(currentUser, dataset, doc, cds, st);
				boolean ok = false;
				if (f != null) {
					ok = f.delete();
					if (ok) {
						logger.info("Deleted file " + f.getAbsolutePath());
					}
					
				}
				if (!ok) {
					logger.warn("Failed to delete zip distribution " + doc.getIdentifier() + " for dataset " + dataset.getUuid());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}		
		
		folderService.deleteDatasetsDistributionFolderIfEmpty(currentUser, dataset);

		return true;
	}	
	
	@Override
	public ListPage<DistributionDocument> getAllByUser(ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(distributionDocumentRepository.findByDatabaseIdAndUserId(database.getId(), userId));
		} else {
			return ListPage.create(distributionDocumentRepository.findByDatabaseIdAndUserId(database.getId(), userId, page));
		}
	}

	@Override
	public ListPage<DistributionDocument> getAllByUser(List<Dataset> dataset, ObjectId userId, Pageable page) {
		if (page == null) {
			if (userId != null) {
				return ListPage.create(distributionDocumentRepository.findByDatasetIdInAndUserId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId));
			} else {
				return ListPage.create(distributionDocumentRepository.findByDatasetIdIn(dataset.stream().map(p -> p.getId()).collect(Collectors.toList())));
			}
		} else {
			if (userId != null) {
				return ListPage.create(distributionDocumentRepository.findByDatasetIdInAndUserId(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), userId, page));
			} else {
				return ListPage.create(distributionDocumentRepository.findByDatasetIdIn(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()), page));
			}
		}
	}

}