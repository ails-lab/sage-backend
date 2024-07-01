package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetContext;
import ac.software.semantic.model.FileDocument;
import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.ParameterBinding;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.RemoteTripleStore;
import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.ResourceOption;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.TemplateService;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.VocabularyEntityDescriptor;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.state.ThesaurusLoadState;
import ac.software.semantic.model.constants.type.DatasetScope;
import ac.software.semantic.model.constants.type.DatasetType;
import ac.software.semantic.model.constants.type.MappingType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.DatasetPublishState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.notification.PublishNotificationObject;
import ac.software.semantic.payload.request.DatasetUpdateRequest;
import ac.software.semantic.payload.response.DatasetResponse;
import ac.software.semantic.payload.response.FileResponse;
import ac.software.semantic.payload.response.MappingResponse;
import ac.software.semantic.payload.response.ProjectDocumentResponse;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.ResponseTaskObject;
import ac.software.semantic.payload.response.TemplateResponse;
import ac.software.semantic.payload.response.modifier.DatasetResponseModifier;
import ac.software.semantic.payload.response.modifier.UserResponseModifier;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.FileDocumentRepository;
import ac.software.semantic.repository.core.IndexDocumentRepository;
import ac.software.semantic.repository.core.MappingDocumentRepository;
import ac.software.semantic.repository.core.TaskRepository;
import ac.software.semantic.repository.root.TemplateServiceRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.DistributionService.DistributionContainer;
import ac.software.semantic.service.FileService.FileContainer;
import ac.software.semantic.service.IndexService.IndexContainer;
import ac.software.semantic.service.MappingService.MappingContainer;
import ac.software.semantic.service.ProjectService.ProjectContainer;
import ac.software.semantic.service.UserService.UserContainer;
import ac.software.semantic.service.container.MemberContainer;
import ac.software.semantic.service.container.MultipleResponseContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.EnclosedObjectContainer;
import ac.software.semantic.service.container.ExecutableContainer;
import ac.software.semantic.service.container.GroupingContainer;
import ac.software.semantic.service.container.IdentifierCachable;
import ac.software.semantic.service.container.InverseMemberContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.PublishableContainer;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.TaskFailureException;
import ac.software.semantic.service.lookup.DatasetLookupProperties;
import ac.software.semantic.service.monitor.GenericMonitor;

@Service
public class DatasetService implements PublishingService<Dataset, DatasetResponse>, 
                                       LookupService<Dataset, DatasetResponse, DatasetLookupProperties>,
									   EnclosingLookupService<Dataset, DatasetResponse, ProjectDocument, DatasetLookupProperties>,
                                       EnclosedCreatableService<Dataset, DatasetResponse, DatasetUpdateRequest, ProjectDocument>,
                                       IdentifiableDocumentService<Dataset, DatasetResponse>
{

	private Logger logger = LoggerFactory.getLogger(DatasetService.class);
	
	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private TripleStore tripleStore;

	@Autowired
	private DatasetRepository datasetRepository;

	@Autowired
	private MappingDocumentRepository mappingRepository;

	@Autowired
	private FileDocumentRepository fileRepository;

	@Autowired
	private IndexDocumentRepository indexRepository;

	@Autowired
	private TemplateServiceRepository templateRepository;

	@Lazy
	@Autowired
	private DistributionService distributionService;

	@Lazy
	@Autowired
	private UserTaskService userTaskService;

//	@Lazy
	@Autowired
	private MappingService mappingService;

//	@Lazy
	@Autowired
	private IndexService indexService;

	@Autowired
	private ProjectService projectService;

	@Autowired
	private PrototypeService prototypeService;

	@Autowired
	private AnnotatorService annotatorService;

	@Autowired
	private TemplateServicesService templateService;

	@Autowired
	private FileService fileService;

	@Autowired
	private LodViewService lodViewService;
	
	@Autowired
	private TaskRepository taskRepository;

	@Autowired
	private TaskService taskService;

	@Autowired
	private UserService userService;

	@Autowired
	private SchemaService schemaService;

	@Autowired
	private IdentifiersService identifierService;
	
	@Autowired
	private APIUtils apiUtils;

    @Autowired
    @Qualifier("database")
    private Database database;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

	@Autowired
	private IdentifiersService idService;

	@Autowired
	private ServiceUtils serviceUtils;
	
	@Override
	public Class<? extends EnclosedObjectContainer<Dataset,DatasetResponse,Dataset>> getContainerClass() {
		return DatasetContainer.class;
	}
	
	@Override 
	public DocumentRepository<Dataset> getRepository() {
		return datasetRepository;
	}
	
	public class DatasetContainer extends EnclosedObjectContainer<Dataset,DatasetResponse,Dataset> 
	                              implements PublishableContainer<Dataset, DatasetResponse, MappingExecuteState,DatasetPublishState,Dataset>,
	                                         UpdatableContainer<Dataset, DatasetResponse, DatasetUpdateRequest>,
	                                         MemberContainer<Dataset, DatasetResponse, Dataset>, 
	                                         InverseMemberContainer<Dataset, DatasetResponse, ProjectDocument>,
	                                         MultipleResponseContainer<Dataset, DatasetResponse, DatasetResponseModifier>, 
	                                         IdentifierCachable<Dataset>, 
	                                         GroupingContainer<Dataset> {
	
		private ObjectId datasetId;
		
		private TripleStoreConfiguration tripleStoreConfiguration;
		
		private DatasetPublishState publishState;
		
		private Map<String, MappingDocument> mappingsMap;
		
		public DatasetContainer(UserPrincipal currentUser, ObjectId datasetId) {
			this.datasetId = datasetId;
			this.currentUser = currentUser;
			
			load();
		}
		
		public DatasetContainer(UserPrincipal currentUser, Dataset dataset) {
			this.datasetId = dataset.getId();
			this.currentUser = currentUser;
			
			this.dataset = dataset;
			this.object = dataset;
			
			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
				publishState = dataset.checkPublishState(vc.getId());
				if (publishState != null) {
					tripleStoreConfiguration = vc;
					break;
				}
			}
		}
		
		@Override 
		public void setObjectOwner() {
			serviceUtils.setObjectOwner(this);
		}
		
		@Override
		public ObjectId getPrimaryId() {
			return datasetId;
		}
		
		@Override 
		public DocumentRepository<Dataset> getRepository() {
			return datasetRepository;
		}

		@Override
		public DatasetService getService() {
			return DatasetService.this;
		}

		@Override 
		public DocumentRepository<Dataset> getEnclosingDocumentRepository() {
			return datasetRepository;
		}
		
		@Override
		public void load() {
			
//			Optional<Dataset> doc = datasetRepository.findByIdAndUserId(datasetId, new ObjectId(currentUser.getId())); // not working for campaigns
			Optional<Dataset> doc = datasetRepository.findById(datasetId);
			
			if (!doc.isPresent()) {
				System.out.println("NOT FOUND");
				return ;
			}
			
			dataset = doc.get();
			object = dataset;
			
			for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
				publishState = dataset.checkPublishState(vc.getId());
				if (publishState != null) {
					tripleStoreConfiguration = vc;
					break;
				}
			}
		}

		@Override
		protected void loadDataset() {
		}
		
		public ObjectId getDatasetId() {
			return datasetId;
		}

		public TripleStoreConfiguration getTripleStoreConfiguration() {
			return tripleStoreConfiguration;
		}

		public void addMapping(String name, MappingDocument mapping) {
			if (mappingsMap == null) {
				mappingsMap = new HashMap<>();
			}
			mappingsMap.put(name, mapping);
		}
		
		public MappingDocument getMapping(String name) {
			if (mappingsMap == null) {
				return null;
			}
			return mappingsMap.get(name);
		}
		
		@Override
		public String localSynchronizationString() {
			return (tripleStoreConfiguration != null ? tripleStoreConfiguration.getId() : "") + ":" + getDatasetId();
		}
		
		@Override
		public Dataset getEnclosingObject() {
			return getObject();
		}

		@Override
		public Dataset update(DatasetUpdateRequest ur) throws Exception {

			update(ids -> {
				Dataset ds = ids.getObject();
				
				ds.setName(ur.getName());
				ds.setIdentifier(ur.getIdentifier());
				ds.setScope(ur.getScope());
				ds.setDatasetType(ur.getType());
				ds.setAsProperty(ur.getAsProperty());
				ds.setPublik(ur.isPublik());
				ds.setMultiGroup(ur.isMultiGroup());
				ds.setPublicGroups(ur.getPublicGroups());
				
				ds.setLinks(ur.getLinks());
				
				ds.setRemoteTripleStore(ur.getRemoteTripleStore());
				
				ds.setEntityDescriptors(ur.getEntityDescriptors());
			});
			
			TemplateService template = null;
			if (getObject().getTemplateId() != null) {
				template = templateRepository.findById(dataset.getTemplateId()).get();
			}
			
			if (template != null) {
		     	TaskDescription tdescr = taskService.newTemplateDatasetUpdateTask(this);
		     	taskService.call(tdescr);
			}
			
			return getObject();
		}
		
		@Override
		public DatasetResponse asResponse(DatasetResponseModifier rm) {

	    	DatasetResponse response = new DatasetResponse();

	    	response.setId(dataset.getId().toString());
	    	response.setUuid(dataset.getUuid());
	    	response.setName(dataset.getName());
	    	response.setIdentifier(dataset.getIdentifier());
	    	response.setPublik(dataset.isPublik());
	    	response.setMultiGroup(dataset.isMultiGroup());
	    	response.setPublicGroups(dataset.getPublicGroups());
	    	
			if (rm == null || rm.getCategory() == ResponseFieldType.EXPAND) {
				response.setCategory(dataset.getCategory());
			}
				
			if (rm == null || rm.getScope() == ResponseFieldType.EXPAND) {
				response.setScope(dataset.getScope());
			}
				
			if (rm == null || rm.getType() == ResponseFieldType.EXPAND) {
				response.setType(dataset.getDatasetType());
			}
				
			if (rm == null || rm.getTypeUri() == ResponseFieldType.EXPAND) {
//				response.setTypeUri(dataset.getTypeUri());
				response.setTags(dataset.getTags());
			}
				
			if (rm == null || rm.getRemoteTripleStore() == ResponseFieldType.EXPAND) {
				response.setRemoteTripleStore(dataset.getRemoteTripleStore());
			}
			
			if (rm == null || rm.getEntityDescriptors() == ResponseFieldType.EXPAND) {
				response.setEntityDescriptors(dataset.getEntityDescriptors());
			}

			if (rm == null || rm.getDates() == ResponseFieldType.EXPAND) {
		    	response.setCreatedAt(dataset.getCreatedAt());
		    	response.setUpdatedAt(dataset.getUpdatedAt());
			}
				
			if (rm == null || rm.getLinks() == ResponseFieldType.EXPAND) {
				if (dataset.getLinks() != null) {
					List<ResourceOption<String>> links = new ArrayList<>();
					for (ResourceOption<ObjectId> ro : dataset.getLinks()) {
						ResourceOption<String> sro = new ResourceOption<>();
						sro.setType(ro.getType());
						sro.setValue(ro.getValue().toString());
						links.add(sro);
					}
					response.setLinks(links);
				}
			}
	
			if (rm == null || rm.getTemplate() == ResponseFieldType.EXPAND) {
				TemplateService template = null;
				if (getObject().getTemplateId() != null) {
					template = templateRepository.findById(dataset.getTemplateId()).get();
				}
					
			    if (template != null) {
			    	response.setTemplate(modelMapper.template2TemplateResponse(template));
			    }
			}
				
			if (rm == null || rm.getStates() == ResponseFieldType.EXPAND) {
				ThesaurusLoadState st = null;
//				if (dataset.getTypeUri().contains(SEMAVocabulary.ThesaurusCollection.toString())) {
				if (dataset.getDatasetType() == DatasetType.DATASET && dataset.getScope() == DatasetScope.VOCABULARY) {
					try {
						st = annotatorService.isLoaded(dataset);
				   	} catch (Exception ex) {
				   		ex.printStackTrace();
				   		st = ThesaurusLoadState.UNKNOWN;
				   	}
				}
					
			    response.setLoadState(ResponseTaskObject.create(st));
		    	
			    response.copyStates(dataset, dataset.getPublishVirtuosoConfiguration(virtuosoConfigurations.values()), fileSystemConfiguration);
			}
	
		    if (rm == null || rm.getDatasets() == ResponseFieldType.EXPAND) {
		    	if (dataset.getDatasets() != null) {
			    	for (ObjectId did : dataset.getDatasets()) {
		    			DatasetContainer innerDataset = getContainer(currentUser, did);
		    			if (innerDataset != null) {
			    			response.addDataset(innerDataset.asResponse(DatasetResponseModifier.baseModifier()));
			    		}
			    	}
		    	}
		    }
		    
			if (rm == null || rm.getProjects() == ResponseFieldType.EXPAND) {
				if (object.getProjectId() != null) {
					List<ProjectDocumentResponse> projects = new ArrayList<>();
					
					for (ObjectId pid : object.getProjectId()) {
						ProjectContainer pc = projectService.getContainer(currentUser, new SimpleObjectIdentifier(pid));
						
	//					ProjectResponseModifier urm = new UserResponseModifier();
	//					urm.setId(ResponseFieldType.IGNORE);
	//					urm.setEmail(ResponseFieldType.IGNORE);
	//					urm.setRoles(ResponseFieldType.IGNORE);
						
						if (pc != null) {
							projects.add(pc.asResponse());
						}
					}
					
					if (projects.size() > 0) {
						response.setProjects(projects);
					}
				}
			}
			
//			List<MappingResponse> mappings = apiUtils.getAllByUser(currentUser, datasetId, mappingService, DatasetService.this).getData();
			List<MappingResponse> mappings = apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId}), mappingService, DatasetService.this).getData();
			List<FileResponse> files = apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId}), fileService, DatasetService.this).getData();
			
			int maxGroup = 0;
			for (MappingResponse mr : mappings) {
				maxGroup = Math.max(maxGroup, mr.getGroup());
			}
			for (FileResponse fr : files) {
				maxGroup = Math.max(maxGroup, fr.getGroup());
			}
			response.setMaxGroup(maxGroup);

		    if (rm != null) {
		    	
				if (rm.getUser() == ResponseFieldType.EXPAND) {
					UserContainer uc = userService.getContainer(currentUser, new SimpleObjectIdentifier(object.getUserId()));
					
					UserResponseModifier urm = new UserResponseModifier();
					urm.setId(ResponseFieldType.IGNORE);
					urm.setEmail(ResponseFieldType.IGNORE);
					urm.setRoles(ResponseFieldType.IGNORE);
					
					if (uc != null) {
						response.setUser(uc.asResponse(urm));
					}
				}
				
				if (rm.getMappings() == ResponseFieldType.EXPAND) {
					response.setMappings(mappings);
				}

				
				if (rm.getRdfFiles() == ResponseFieldType.EXPAND) {
					response.setRdfFiles(files);
				}

				if (rm.getDistributions() == ResponseFieldType.EXPAND) {
//					response.setDistributions(apiUtils.getAllByUser(currentUser, datasetId, distributionService, DatasetService.this).getData());
					response.setDistributions(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId}), distributionService, DatasetService.this).getData());
				}
				
				if (rm.getUserTasks() == ResponseFieldType.EXPAND) {
//					response.setUserTasks(apiUtils.getAllByUser(currentUser, datasetId, userTaskService, DatasetService.this).getData());
					response.setUserTasks(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId}), userTaskService, DatasetService.this).getData());
				}
				
				if (rm.getIndices() == ResponseFieldType.EXPAND) {
//					response.setIndices(apiUtils.getAllByUser(currentUser, datasetId, indexService, DatasetService.this).getData());
					response.setIndices(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId}), indexService, DatasetService.this).getData());
				}
				
//				if (rm.getPrototypes() == ResponseFieldType.EXPAND) {
//					//response.setRdfFiles(apiUtils.getAllByUser(currentUser, datasetId, fileService, DatasetService.this).getData());
//					response.setPrototypes(apiUtils.getAllByUser(currentUser, null, Arrays.asList(new ObjectId[] {datasetId}), prototypeService, DatasetService.this).getData());
//				}

			}
			
			return response;
		}
		
		@Override
		public void updateMaxGroup() throws Exception {
			int maxGroup = 0;
			for (MappingDocument doc : mappingRepository.findByDatasetId(datasetId)) {
				maxGroup = Math.max(maxGroup, doc.getGroup());
			}
			
			for (FileDocument doc : fileRepository.findByDatasetId(datasetId)) {
				maxGroup = Math.max(maxGroup, doc.getGroup());
			}

			for (IndexDocument doc : indexRepository.findByDatasetId(datasetId)) {
				maxGroup = Math.max(maxGroup, doc.getGroup());
			}

			final int group = maxGroup;
			
			update(ioc -> {
				Dataset dataset = ioc.getObject();
				dataset.setMaxGroup(group);
			});

		}
		
		@Override
		public String getDescription() {
			return object.getName();
		}

		@Override 
		public void publish(Properties props) throws Exception {
			tripleStore.publishDatasetMetadata(this);
		}

		@Override 
		public void unpublish(Properties props) throws Exception {
			tripleStore.unpublish(this, props.get(ServiceProperties.METADATA) == ServiceProperties.ALL, props.get(ServiceProperties.CONTENT) == ServiceProperties.ALL, (Integer)props.get(ServiceProperties.DATASET_GROUP));
		}

		@Override
		public TaskType getPublishTask() {
			return TaskType.DATASET_PUBLISH;
		}

		@Override
		public TaskType getUnpublishTask() {
			return TaskType.DATASET_UNPUBLISH;
		}

		@Override
		public TaskType getRepublishTask() {
			return TaskType.DATASET_REPUBLISH;
		}
		
		@Override
		public boolean isPublished(Properties props) {
			
			ProcessStateContainer<DatasetPublishState> psc = object.getCurrentPublishState(getVirtuosoConfigurations().values());
			if (psc != null) {
				DatasetPublishState ps = psc.getProcessState();
				
				Integer group = (Integer)props.get(ServiceProperties.DATASET_GROUP);

				if (group == -1) {
					if (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC || ps.getPublishState() == DatasetState.PUBLISHED_PRIVATE || ps.getPublishState() == DatasetState.PUBLISHED) {
						return true;
					}
				} else {
					List<DatasetPublishState> groups = ps.getGroups();
					if (groups != null) {
						for (DatasetPublishState gr : groups) {
							if (gr.getGroup() == group) {
								if (gr.getPublishState() == DatasetState.PUBLISHED_PUBLIC || gr.getPublishState() == DatasetState.PUBLISHED_PRIVATE || gr.getPublishState() == DatasetState.PUBLISHED) {
									return true;
								}
							}
						}
					}
				}
			} 
				
			return false;
		}
		
		@Override
		public boolean isFailed(Properties props) {
			ProcessStateContainer<DatasetPublishState> psc = object.getCurrentPublishState(getVirtuosoConfigurations().values());
			if (psc != null) {
				DatasetPublishState ps = psc.getProcessState();
				
				Integer group = (Integer)props.get(ServiceProperties.DATASET_GROUP);

				if (group == -1) {
					if (ps.getPublishState() == DatasetState.UNPUBLISHING_FAILED || ps.getPublishState() == DatasetState.PUBLISHING_FAILED) {
						return true;
					}
				} else {
					List<DatasetPublishState> groups = ps.getGroups();
					if (groups != null) {
						for (DatasetPublishState gr : groups) {
							if (gr.getGroup() == group) {
								if (gr.getPublishState() == DatasetState.UNPUBLISHING_FAILED || gr.getPublishState() == DatasetState.PUBLISHING_FAILED) {
									return true;
								}
							}
						}
					}
				}
			}
				
			return false;
		}

		
//		@Override
		public void removePublishState(DatasetPublishState ps, GenericMonitor pm, Properties props) {
			Integer group = (Integer)props.get(ServiceProperties.DATASET_GROUP);
			
			if (group == -1) {
				if (ps.getGroups() == null) {
					object.removePublishState(ps);
				} else {
					boolean published = false;
					boolean publishing = false;
					boolean publishingFailed = false;
					boolean unpublishing = false;
					boolean unpublishingFailed = false;
					
					for (DatasetPublishState dps: ps.getGroups()) {
						DatasetState state = dps.getPublishState();
						if (state == DatasetState.PUBLISHED || state == DatasetState.PUBLISHED_PRIVATE || state == DatasetState.PUBLISHED_PUBLIC) {
							published = true;
						} else if (state == DatasetState.PUBLISHING) {
							publishing = true; // ??
						} else if (state == DatasetState.PUBLISHING_FAILED) {
							publishingFailed = true;
						} else if (state == DatasetState.UNPUBLISHING) {
							unpublishing = true; // ??
						} else if (state == DatasetState.UNPUBLISHING_FAILED) {
							unpublishingFailed = true;
						}
					}
					
					if (unpublishingFailed) {
						ps.failUndo(pm);
					} else if (publishingFailed) {
						ps.failDo(pm);
					} else if (published) {
						ps.completeDo(pm);
					}
				}
			} else {
				ps.removeGroup(group);
			}
		}
		
		@Override
		public TaskDescription getActiveTask(TaskType type) {
			if (type == TaskType.DATASET_EXECUTE_MAPPINGS || type == TaskType.DATASET_EXECUTE_ANNOTATORS) {
				return taskRepository.findActiveByDatasetIdAndFileSystemConfigurationId(getObject().getId(), fileSystemConfiguration.getId(), type).orElse(null);
			} else {
				return taskRepository.findActiveByDatasetIdAndTripleStoreConfigurationId(getObject().getId(), getTripleStoreConfiguration() != null ? getTripleStoreConfiguration().getId() : null, type).orElse(null);
			} 
		}
		
		@Override
		public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations() {
			return virtuosoConfigurations;
		}	

		@Override
		public boolean delete() throws Exception {

			synchronized (saveSyncString()) { // wrong synchronization should synch on individual affected objects

				//TODO: should delete catalog members
				for (MappingDocument mdoc : mappingRepository.findByDatasetId(getDatasetId())) {
					MappingContainer mc = mappingService.getContainer(getCurrentUser(), dataset, mdoc, null);
					mc.delete();
				}
				
//				List<MappingDocument> datasetMappings = mappingRepository.findByDatasetIdAndUserId(datasetId, new ObjectId(currentUser.getId()));
//				for (MappingDocument map : datasetMappings) {
//					mappingService.deleteMapping(currentUser, map.getId());
//				}
	
				datasetRepository.deleteByIdAndUserId(datasetId, new ObjectId(currentUser.getId()));
	
				for (Dataset ds : datasetRepository.findByDatasets(datasetId)) {
					ds.getDatasets().remove(datasetId);
					datasetRepository.save(ds);
				}
			}

			return true;
		}

		public List<ObjectContainer> getActiveInnerContainers(Class<? extends ObjectContainer> container) {
			List<ObjectContainer> res = new ArrayList<>();
			
			if (container == MappingContainer.class) {
				res.addAll(getActiveMappingContainers());
			} else if (container == FileContainer.class) {
				res.addAll(getInnerContainers(fileService));
			} else if (container == IndexContainer.class) {
				res.addAll(getInnerContainers(indexService));
			} else if (container == DistributionContainer.class) {
				res.addAll(getInnerContainers(distributionService));
			}
			
			return res;
		}
		
		public List<MappingContainer> getActiveMappingContainers() {
			List<MappingContainer> res = new ArrayList<>();
			
			for (MappingDocument mdoc : mappingRepository.find(null, Arrays.asList(new Dataset[] {dataset }), null, database.getId())) {
				if (mdoc.isActive()) {
					for (MappingInstance mi : mdoc.getInstances()) {
						if (mi.isActive() || !mdoc.hasParameters()) {
							res.add(mappingService.getContainer(getCurrentUser(), dataset, mdoc, mi));
						}
					}
				}
			}
			
			return res;
		}
		
		public <D extends SpecificationDocument, F extends Response>
		List<ObjectContainer<D,F>> getInnerContainers(EnclosingService<D,F,Dataset> service) {
			List<ObjectContainer<D,F>> res = new ArrayList<>();
			
			for (D idoc : service.getAllByUser(Arrays.asList(new Dataset[] {dataset }), getCurrentUser() != null ? new ObjectId(getCurrentUser().getId()) : null, null).getList()) {
				res.add(service.getContainer(getCurrentUser(), idoc));
			}
			
			return res;
		}
		
		public FileSystemConfiguration getFileSystemConfiguration() {
			return fileSystemConfiguration;
		}

		@Override
		public void removeFromCache() {
			identifierService.remove(getObject());
		}
	}
	
	@Override
	public String synchronizedString(String id) {
		return serviceUtils.syncString(id, getContainerClass());
	}

	public DatasetContainer getInDatasetContainer(EnclosedObjectContainer<?,?,Dataset> oc) {
		if (oc instanceof DatasetContainer) {
			return (DatasetContainer)oc;
		} else {
			return new DatasetContainer(oc.getCurrentUser(), oc.getEnclosingObject());
		}
	}
	
	// to be called only by api create dataset
	@Override
	public Dataset create(UserPrincipal currentUser, ProjectDocument inside, DatasetUpdateRequest ur) throws Exception {
		TemplateResponse template = ur.getTemplate();
		
		ObjectId templateId = null; 
		List<ParameterBinding> templateBindings = null;
    	
    	if (template != null) {
			if (templateService.getDatasetImportTemplate(new ObjectId(template.getId()), ur.getType()) == null) {
				throw new Exception("Template not found");
			} else {
				templateId = new ObjectId(template.getId());
				
				if (template.getParameters() != null && template.getParameters().size() > 0) {
					templateBindings = new ArrayList<>();
					for (ParameterBinding pb : template.getParameters()) {
						templateBindings.add(new ParameterBinding(pb.getName(), pb.getValue()));
					}
				}
			}
    	}
    	
//		String typeUri = ur.getTypeUri() != null && ur.getTypeUri().size() > 0 ? ur.getTypeUri().get(0) : null;
    	String typeUri = null;
		
		Dataset dataset = createDataset(currentUser, ur.getName(), ur.getIdentifier(), ur.isPublik(), ur.isMultiGroup(), ur.getPublicGroups(), ur.getScope(), ur.getType(), typeUri, ur.getAsProperty(), ur.getLinks(), templateId, templateBindings, ur.getRemoteTripleStore(), ur.getEntityDescriptors(), inside);
		
		if (template != null) {
			DatasetContainer dc = getContainer(currentUser, dataset.getId());
			
	     	TaskDescription tdescr = taskService.newTemplateDatasetTask(dc);
	     	taskService.call(tdescr);
		}
		
		return dataset;
	}

	public Dataset createDataset(UserPrincipal currentUser, String name, String identifier, boolean publik, boolean multiGroup, List<Integer> publicGroups, DatasetScope scope, DatasetType type, String typeUri, String asProperty, List<ResourceOption<ObjectId>> links, ObjectId templateId, List<ParameterBinding> templateBindings, RemoteTripleStore remoteTripleStore, List<VocabularyEntityDescriptor> entityDescriptors, ProjectDocument project ) throws Exception{
		
		Dataset ds = new Dataset(database);
		ds.setUserId(new ObjectId(currentUser.getId()));

		ds.setName(name);
		ds.setIdentifier(identifier);
		ds.setScope(scope);
		ds.setDatasetType(type);
		ds.setAsProperty(asProperty);
		ds.setPublik(publik);
		ds.setMultiGroup(multiGroup);
		ds.setPublicGroups(publicGroups);
		
		if (templateId != null) {
			ds.setTemplateId(templateId);
			ds.setBinding(templateBindings);
		}

		if (links != null && links.size() > 0 ) {
			ds.setLinks(links);
		}
		
		if (entityDescriptors != null && entityDescriptors.size() > 0) {
			ds.setEntityDescriptors(entityDescriptors);
		}
		
		ds.setRemoteTripleStore(remoteTripleStore);
		
		if (project != null) {
			ds.setProjectId(Arrays.asList(new ObjectId[] { project.getId() }));
		}
		
		return create(ds);
	}
	
	public Optional<Dataset> getDataset(UserPrincipal currentUser, ObjectId id) {
		return datasetRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId()));
	}
	
	public List<MappingDocument> getMappings(UserPrincipal currentUser, ObjectId id) {
		return mappingRepository.findByDatasetIdAndUserId(id, new ObjectId(currentUser.getId()));
	}
	
	public boolean remove(UserPrincipal currentUser, String id, String fromId) {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(fromId), new ObjectId(currentUser.getId()));

		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		dataset.removeDataset(new ObjectId(id));
		
		datasetRepository.save(dataset);
		
		return true;
	}
	

	public DatasetContainer getContainer(UserPrincipal currentUser, ObjectId datasetId) {
		DatasetContainer dc = new DatasetContainer(currentUser, datasetId);
		if (dc.getObject() == null) {
			return null;
		} else {
			return dc;
		}
	}

	@Override
	public DatasetContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		DatasetContainer dc = new DatasetContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());
		if (dc.getObject() == null) {
			return null;
		} else {
			return dc;
		}
	}
	
	@Override
	public DatasetContainer getContainer(UserPrincipal currentUser, Dataset dataset) {
		DatasetContainer dc = new DatasetContainer(currentUser, dataset);
		if (dc.getObject() == null) {
			return null;
		} else {
			return dc;
		}
	}
	
	@Override
	public ObjectContainer<Dataset,DatasetResponse> getContainer(UserPrincipal currentUser, Dataset dataset, ProjectDocument project) {
		DatasetContainer dc = new DatasetContainer(currentUser, dataset);
		if (dc.getObject() == null) {
			return null;
		} else {
			return dc;
		}
	}
	
	@Override
	public Date prePublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("prePublish " + tdescr.getType());
 		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		PublishableContainer<?,?,?,?,?> pc = (PublishableContainer<?,?,?,?,?>)tdescr.getContainer();
 		
 		TripleStoreConfiguration vc = (TripleStoreConfiguration)tdescr.getProperties().get(ServiceProperties.TRIPLE_STORE);
 		
// 		System.out.println("prePublish 1");
		try {
			pc.update(idc -> {			
				DatasetPublishState pes = ((DatasetContainer)idc).getPublishDocument().getPublishState(vc.getId()); 
				pes.startDo(pm);
			});
			
//			System.out.println("prePublish 2");

			DatasetContainer dc = (DatasetContainer)tdescr.getContainer();

			idService.remove(dc.getEnclosingObject());
			
			pm.sendMessage(new PublishNotificationObject(pc));
			
			logger.info("Publication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " started.");
			
			return new Date();
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
//			System.out.println("prePublish 3");

			try {
				pc.update(ioc -> {
					PublishState<ExecuteState> ips = ((PublishableContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm);
					}
				});
				
//				System.out.println("prePublish 4");
				
				if (pc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(pc));
				}
				
			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}

	
	// corresponds to publish metadata : content is published by mapping/file publish
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> publish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("publish " + tdescr.getType());
		
		GenericMonitor pm = (GenericMonitor)tdescr.getParent().getMonitor();

		PublishableContainer<?,?,ExecuteState,PublishState<ExecuteState>,?> pc = (PublishableContainer)tdescr.getContainer();
		
//		System.out.println("publish 1");
		try {
			pc.publish(null);
			
//			System.out.println("publish 2");

			return new AsyncResult<>(new Date());
			
		} catch (Exception ex) {
			ex.printStackTrace();

			pm.complete(ex);
			
//			System.out.println("publish 3");

			try {
				pc.update(ioc -> {
					PublishState<ExecuteState> ips = ((PublishableContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm);
					}
				});
				
//				System.out.println("publish 4");
				
				if (pc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(pc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}	

	@Override
	public Date postPublishSuccess(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("postPublishSuccess " + tdescr.getType());
 		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		PublishableContainer<?,?,?,?,?> pc = (PublishableContainer<?,?,?,?,?>)tdescr.getContainer();
 		
// 		System.out.println("postPublishSuccess 1");
 		
		try { 		
			DatasetContainer dc = (DatasetContainer)tdescr.getContainer();
			Dataset dataset = dc.getObject();
			UserPrincipal currentUser = dc.getCurrentUser();
			TripleStoreConfiguration vc = dc.getDatasetTripleStoreVirtuosoConfiguration();

//			System.out.println("postPublishSuccess 2");
			
			if (dataset.getDatasetType() == DatasetType.DATASET && dataset.getScope() == DatasetScope.VOCABULARY) {
				annotatorService.unload(dataset);
			}
			
			pm.complete();
			
//			System.out.println("postPublishSuccess 3");
			
			tripleStore.setPublishLastModified(currentUser, vc, dataset, pm.getCompletedAt());
			
			pc.update(ioc -> {
				PublishableContainer<?,?,?,DatasetPublishState,?> ipc = (PublishableContainer<?,?,?,DatasetPublishState,?>)ioc;

				Dataset idataset = ((DatasetContainer)ioc).getObject();
				DatasetPublishState ips = ipc.getPublishState();

				ips.completeDo(pm);
				ips.setUriSpaces(schemaService.getUriSpaces(idataset));
				
				if (idataset.checkFirstPublishState(vc.getId()) == null) {
					idataset.setFirstPublishState(ips, vc.getId());
				}
			});
		
			logger.info("Publication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(pc));
			
			lodViewService.updateLodView();
			
			return pm.getCompletedAt();
		
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
//			System.out.println("postPublishSuccess 4");
			
			try {
				pc.update(ioc -> {
					PublishState<ExecuteState> ips = ((PublishableContainer)ioc).checkPublishState();
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
	
	@Override
	public Date postPublishFail(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("postPublishFail " + tdescr.getType());
		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		PublishableContainer<?,?,?,?,?> pc = (PublishableContainer<?,?,?,?,?>)tdescr.getContainer();
 		
// 		System.out.println("postPublishFail 1");
 		
		try {
			
			pm.complete();
			
//			System.out.println("postPublishFail 2");
			
			pc.update(idc -> {			
				PublishState<?> pes = ((DatasetContainer)idc).getPublishState(); 
				pes.failDo(pm);
			});
			
			if (pc.checkPublishState() != null) {
				pm.sendMessage(new PublishNotificationObject(pc));
			}

	 		lodViewService.updateLodView();
	 		

//			System.out.println("postPublishFail 3");
			
			return pm.getCompletedAt();
		
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
//			System.out.println("postPublishFail 4");
			try {
				pc.update(ioc -> {
					PublishState<ExecuteState> ips = ((PublishableContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failDo(pm);
					}
				});
				
//				System.out.println("postPublishFail 5");
				
				if (pc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(pc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	@Override
	public Date preUnpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("preUnpublish " + tdescr.getType());
		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		PublishableContainer<?,?,?,?,?> pc = (PublishableContainer<?,?,?,?,?>)tdescr.getContainer();
 		
// 		System.out.println("preUnpublish 1");
		try {
			
			pc.update(idc -> {			
				PublishState<?> pes = ((PublishableContainer<?,?,?,?,?>)idc).getPublishState(); 
				pes.startUndo(pm);
			});

			DatasetContainer dc = (DatasetContainer)tdescr.getContainer();

//			System.out.println("preUnpublish 2");
			
			idService.remove(dc.getEnclosingObject());
			
			pm.sendMessage(new PublishNotificationObject(pc));
		
			logger.info("Unpublication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " started.");
			
			return new Date();
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);

//			System.out.println("preUnpublish 3");
			
			try {
				pc.update(ioc -> {
					PublishState<?> ips = ((PublishableContainer<?,?,?,?,?>)ioc).checkPublishState();
					if (ips != null) {
						ips.failUndo(pm);
					}
				});
				
//				System.out.println("preUnpublish 4");
				
				if (pc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(pc));
				}
				
			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}

	// corresponds to publish both metadata and content through props
	@Override
	@Async("publishExecutor")
	public ListenableFuture<Date> unpublish(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("unpublish " + tdescr.getType());
		GenericMonitor pm = (GenericMonitor)tdescr.getParent().getMonitor();	

		DatasetContainer dc = (DatasetContainer)tdescr.getContainer();
		Dataset dataset = dc.getObject();

		boolean content = tdescr.getType() == TaskType.DATASET_UNPUBLISH_CONTENT;
		boolean metadata = tdescr.getType() == TaskType.DATASET_UNPUBLISH_METADATA;

//		System.out.println("unpublish 1");
		
		PublishableContainer<?,?,?,DatasetPublishState,Dataset> pc = (PublishableContainer)tdescr.getContainer();
		
		try {
//			logger.info("Unpublication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " started.");

			Set<Integer> unpublishGroups = new TreeSet<>();
			
			Integer singleGroup = (Integer)tdescr.getProperties().get(ServiceProperties.DATASET_GROUP);
			if (singleGroup != -1) {
				unpublishGroups.add(singleGroup);
			} else {
//				for (int i = 0; i <= dataset.getMaxGroup(); i++) { // to be sure all groups are unpublished
//					unpublishGroups.add(i);
//				}
				DatasetPublishState ps = pc.getPublishState();
				List<DatasetPublishState> groups = ps.getGroups();
				if (groups != null) {
					for (DatasetPublishState dps : groups) {
						unpublishGroups.add(dps.getGroup());
					}
				} else {
					unpublishGroups.add(0);
				}
			}
			
//			System.out.println("unpublish 2");
			
			for (Integer group : unpublishGroups) { 
				for (ObjectContainer<MappingDocument,MappingResponse> mc : dc.getInnerContainers(mappingService)) {
					if (mc.getObject().getGroup() != group) {
						continue;
					}
					
					mc.update(imc -> {
						MappingType type = imc.getObject().getType();
						if ((metadata && type == MappingType.HEADER) || (content && type == MappingType.CONTENT)) {
							for (MappingContainer mic : ((MappingContainer)imc).expandToInstances()) {
								MappingPublishState state = mic.getPublishState();
								state.startUndo(pm);
							}
						}
					});
				}
				
//				System.out.println("unpublish 3");
				
				if (content) {
					for (ObjectContainer<FileDocument,FileResponse> fc : dc.getInnerContainers(fileService)) {
						if (fc.getObject().getGroup() != group) {
							continue;
						}
						
						fc.update(ifc -> {
							PublishState<?> state = ((PublishableContainer<?,?,?,?,?>)ifc).getPublishState();
							state.startUndo(pm);
						});
					}
				}
				
//				System.out.println("unpublish 4");
				
				Properties pp  = new Properties();
				pp.put(ServiceProperties.METADATA, metadata ? ServiceProperties.ALL : ServiceProperties.NONE);
				pp.put(ServiceProperties.CONTENT, content ? ServiceProperties.ALL : ServiceProperties.NONE);
				pp.put(ServiceProperties.DATASET_GROUP, group);
	
				pc.unpublish(pp);
				
				// remove from inknowledge
				if (metadata && dataset.getDatasetType() == DatasetType.DATASET && dataset.getScope() == DatasetScope.VOCABULARY) {
					annotatorService.unload(dataset);
				}
				
//				System.out.println("unpublish 5");
				
				for (ObjectContainer<MappingDocument,MappingResponse> mc : dc.getInnerContainers(mappingService)) {
					if (mc.getObject().getGroup() != group) {
						continue;
					}
					
					mc.update(imc -> {
						MappingDocument mdoc = imc.getObject();
						MappingType type = mdoc.getType();
						
						if ((metadata && type == MappingType.HEADER) || (content && type == MappingType.CONTENT)) {
							for (MappingContainer mic : ((MappingContainer)imc).expandToInstances()) {
								MappingPublishState state = mic.getPublishState();
		
								mic.removePublishState(state);
						
								// remove published execution if newer execution exists. problem if unpublishing from different location than publishing: execution is not available
								MappingExecuteState ies = mic.getExecuteState();
								MappingExecuteState ipes = state.getExecute();
								if (ies != null && ipes != null && ies.getExecuteStartedAt().compareTo(ipes.getExecuteStartedAt()) != 0) {
									mic.clearExecution(ipes);
								}
							}
						}
					});
				}
				
//				System.out.println("unpublish 6");
				
				if (content) {
					for (ObjectContainer<FileDocument,FileResponse> fc : dc.getInnerContainers(fileService)) {
						if (fc.getObject().getGroup() != group) {
							continue;
						}
						
						fc.update(ifc -> {
							PublishableContainer<?,?,ExecuteState, PublishState<ExecuteState>,Dataset> ipc = (PublishableContainer)ifc;
							ExecutableContainer<?,?,ExecuteState,Dataset> iec = (ExecutableContainer)ifc;
							
							PublishState<ExecuteState> ips = ipc.getPublishState();
							
							ipc.removePublishState(ips);
							
							ExecuteState ies = iec.getExecuteState();
							ExecuteState ipes = ips.getExecute();
							if (ies != null && ipes != null && ies.getExecuteStartedAt().compareTo(ipes.getExecuteStartedAt()) != 0) {
								iec.clearExecution(ipes);
							}
						});
					}
				}
				
//				System.out.println("unpublish 7");
				
				if (content) {
					pc.update(ioc -> {
						DatasetContainer ipc = (DatasetContainer)ioc;
						
						DatasetPublishState ips = ipc.getPublishState();
						
						Properties props = new Properties();
						props.put(ServiceProperties.DATASET_GROUP, group);
						
						ipc.removePublishState(ips, pm, props);
					});
					
					pm.sendMessage(new PublishNotificationObject(pc));
				}
			}
			
//			System.out.println("unpublish 8");
//			pm.complete();
			
//			if (metadata) {
//				pc.save(ioc -> {
//					PublishableContainer<?,ExecuteState, PublishState<ExecuteState>,Dataset> ipc = (PublishableContainer)ioc;
//					PublishState<ExecuteState> ips = ipc.getPublishState();
//						
//					ipc.removePublishState(ips);
//				});
//			}
			
//			logger.info("Unpublication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " completed.");
			
//			pm.sendMessage(new PublishNotificationObject(pc));

//			return new AsyncResult<>(pm.getCompletedAt());
			return new AsyncResult<>(new Date());
			
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
//			System.out.println("unpublish 9");
			
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
				
//				System.out.println("unpublish 10");

				for (ObjectContainer<MappingDocument,MappingResponse> mc : dc.getInnerContainers(mappingService)) {
					mc.update(imc -> {
						MappingDocument mdoc = imc.getObject();
						MappingType type = mdoc.getType();
						
						if ((metadata && type == MappingType.HEADER) || (content && type == MappingType.CONTENT)) {
							for (MappingContainer mic : ((MappingContainer)imc).expandToInstances()) {
								MappingPublishState state = mic.getPublishState();
								state.failUndo(pm);
							}
						}
					});
				}
				
				for (ObjectContainer<FileDocument,FileResponse> fc : dc.getInnerContainers(fileService)) {
					fc.update(ifc -> {
						PublishableContainer<?,?,ExecuteState, PublishState<ExecuteState>,Dataset> ipc = (PublishableContainer)ifc;
						PublishState<ExecuteState> ips = ipc.getPublishState();
						ips.failUndo(pm);
					});
				}
				
			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	

	@Override
	public Date postUnpublishSuccess(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("postUnpublishSuccess " + tdescr.getType());
 		
		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		PublishableContainer<?,?,?,?,?> pc = (PublishableContainer<?,?,?,?,?>)tdescr.getContainer();
 		
// 		System.out.println("postUnpublishSuccess 1");
 		
		try {
	 		pm.complete();
	 		
//	 		System.out.println("postUnpublishSuccess 2");
	 		
			pc.update(ioc -> {
				DatasetContainer ipc = (DatasetContainer)ioc;
				
				DatasetPublishState ips = ipc.getPublishState();
				
				Properties props = new Properties();
				props.put(ServiceProperties.DATASET_GROUP, -1);
				
				ipc.removePublishState(ips, pm, props);
			});
		
			logger.info("Unpublication of " + pc.getClass().getName() + ":"  + pc.getPrimaryId() + " completed.");
			
			pm.sendMessage(new PublishNotificationObject(pc));
			
	 		lodViewService.updateLodView();

			return pm.getCompletedAt();
		
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
//			System.out.println("postUnpublishSuccess 3");
			
			try {
				pc.update(ioc -> {
					PublishState<ExecuteState> ips = ((PublishableContainer)ioc).checkPublishState();
					if (ips != null) {
						ips.failUndo(pm);
					}
				});
				
				
//				System.out.println("postUnpublishSuccess 4");
				
				if (pc.checkPublishState() != null) {
					pm.sendMessage(new PublishNotificationObject(pc));
				}

			} catch (Exception iex) {
				throw new TaskFailureException(iex, pm.getCompletedAt());
			}
			
			throw new TaskFailureException(ex, pm.getCompletedAt());
		}
	}
	
	@Override
	public Date postUnpublishFail(TaskDescription tdescr, WebSocketService wsService) throws TaskFailureException {
		System.out.println("postUnpublishFail " + tdescr.getType());
		
 		GenericMonitor pm = (GenericMonitor)tdescr.getMonitor();
 		PublishableContainer<?,?,?,?,?> pc = (PublishableContainer<?,?,?,?,?>)tdescr.getContainer();
 		
// 		lodViewService.updateLodView();
 		
// 		System.out.println("postUnpublishFail 1");
 		
		pm.complete();
			
		try {
			pc.update(idc -> {			
				PublishState<?> pes = ((DatasetContainer)idc).getPublishState(); 
				pes.failUndo(pm);
			});
			
//			System.out.println("postUnpublishFail 2");
			
			if (pc.checkPublishState() != null) {
				pm.sendMessage(new PublishNotificationObject(pc));
			}
			
			return pm.getCompletedAt();
		
		} catch (Exception ex) {
			ex.printStackTrace();
			
			pm.complete(ex);
			
//			System.out.println("postUnpublishFail 3");
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

	@Override
	public ListPage<Dataset> getAll(DatasetLookupProperties lp, Pageable page) {
		return getAll(null, lp, page);
	}

	@Override
	public ListPage<Dataset> getAllByUser(ObjectId userId, Pageable page) {
		return getAllByUser(null, userId, page);
	}

	@Override
	public ListPage<Dataset> getAllByUser(ObjectId userId, DatasetLookupProperties lp, Pageable page) {
		return getAllByUser(null, userId, lp, page);
	}

	@Override
	public ListPage<Dataset> getAll(List<ProjectDocument> project, DatasetLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(datasetRepository.find(null, project, lp, database.getId(), virtuosoConfigurations.ids()));
		} else {
			return ListPage.create(datasetRepository.find(null, project, lp, database.getId(), virtuosoConfigurations.ids(), page));
		}
	}

	@Override
	public ListPage<Dataset> getAllByUser(List<ProjectDocument> project, ObjectId userId, Pageable page) {
		if (page == null) {
			return ListPage.create(datasetRepository.find(userId, project, null, database.getId(), virtuosoConfigurations.ids()));
		} else {
			return ListPage.create(datasetRepository.find(userId, project, null, database.getId(), virtuosoConfigurations.ids(), page));
		}	
	}

	@Override
	public ListPage<Dataset> getAllByUser(List<ProjectDocument> project, ObjectId userId, DatasetLookupProperties lp, Pageable page) {
		if (page == null) {
			return ListPage.create(datasetRepository.find(userId, project, lp, database.getId(), virtuosoConfigurations.ids()));
		} else {
			return ListPage.create(datasetRepository.find(userId, project, lp, database.getId(), virtuosoConfigurations.ids(), page));
		}
	}

	public VocabularyContainer<ResourceContext> createVocabularyContainer(ObjectId datasetId) {
		return createVocabularyContainer(Arrays.asList(new ObjectId[] { datasetId }), new VocabularyContainer<>());
	}

	public VocabularyContainer<ResourceContext> createVocabularyContainer(List<ObjectId> datasetIds) {
		return createVocabularyContainer(datasetIds, new VocabularyContainer<>());
	}

	public VocabularyContainer<ResourceContext> createVocabularyContainer(ObjectId datasetId, VocabularyContainer<ResourceContext> vcont) {
		return createVocabularyContainer(Arrays.asList(new ObjectId[] { datasetId }), vcont);
	}
	
	public VocabularyContainer<ResourceContext> createVocabularyContainer(List<ObjectId> datasetIds, VocabularyContainer<ResourceContext> vcont) {

		for (ObjectId id : datasetIds) {
			DatasetContainer dc = getContainer(null, new SimpleObjectIdentifier(id));
			if (dc != null) {
				Dataset dataset = dc.getObject();
				
				if (dataset.getDatasetType() == DatasetType.DATASET) {
					DatasetPublishState dps = dataset.getCurrentPublishState(virtuosoConfigurations.values()).getProcessState();
					
					DatasetContext ai = new DatasetContext(schemaService);
					ai.setName(dataset.getName());
					ai.setDatasetContainer(dc);
					ai.setUriDescriptors(dps.namespaces2UriDesciptors());
					
					vcont.add(ai);
				} else {
					createVocabularyContainer(dataset.getDatasets(), vcont);
				}
			}
		}
		
		return vcont;
	}

	@Override
	public DatasetLookupProperties createLookupProperties() {
		return new DatasetLookupProperties();
	}

}
