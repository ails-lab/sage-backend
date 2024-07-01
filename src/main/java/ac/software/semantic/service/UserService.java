package ac.software.semantic.service;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.controller.utils.APIUtils;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.Pagination;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.TokenDetails;
import ac.software.semantic.model.TaskDescription;
import ac.software.semantic.model.ActionToken;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.User;
import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.constants.type.TokenState;
import ac.software.semantic.model.constants.type.UserRoleType;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ProjectService.ProjectContainer;
import ac.software.semantic.service.container.MultipleResponseContainer;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.container.SimpleObjectIdentifier;
import ac.software.semantic.service.container.UpdatableContainer;
import ac.software.semantic.service.exception.StateConflictException;
import ac.software.semantic.service.lookup.UserLookupProperties;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.AnnotationEditRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.TokenRepository;
import ac.software.semantic.repository.core.UserRepository;
import ac.software.semantic.repository.core.UserRoleRepository;

import org.apache.commons.validator.EmailValidator;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import ac.software.semantic.payload.request.UserUpdateRequest;
import ac.software.semantic.payload.response.ResponseFieldType;
import ac.software.semantic.payload.response.UserResponse;
import ac.software.semantic.payload.response.modifier.UserResponseModifier;


@Service
public class UserService implements CreatableService<User, UserResponse, UserUpdateRequest>,
                                    LookupService<User, UserResponse, UserLookupProperties>,
                                    IdentifiableDocumentService<User, UserResponse> { 

	Logger logger = LoggerFactory.getLogger(UserService.class);
	
	@Autowired
	private UserRepository userRepository;

	@Autowired
	private PasswordEncoder passwordEncoder;

	@Autowired
	private UserRoleRepository roleRepository;

	@Autowired
	private TokenService tokenService;

	@Autowired
	private ProjectService projectService;

	@Autowired
	private APIUtils apiUtils;

    @Autowired
    @Qualifier("database")
    private Database database;
	
	@Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
	
	@Autowired
	private DatasetRepository datasetRepository;
	
	@Autowired
	private AnnotationEditRepository annotationEditRepository;
    
	@Override
	public Class<? extends ObjectContainer<User, UserResponse>> getContainerClass() {
		return UserContainer.class;
	}
	
	@Override 
	public DocumentRepository<User> getRepository() {
		return userRepository;
	}
	
	@Override
	public IdentifierType[] identifierTypes() {
		return new IdentifierType[] { IdentifierType.IDENTIFIER, IdentifierType.EMAIL };
	}
	
	@Override
	public boolean isValidIdentifier(User doc, IdentifierType type) {
		if (type == IdentifierType.IDENTIFIER) {
			return doc.getIdentifier().matches("^[0-9a-zA-Z\\-\\~\\._:]+$");
		} else if (type == IdentifierType.EMAIL) {
			return EmailValidator.getInstance().isValid(doc.getEmail());
		} else {
			return false;
		}
	}
	
	public class UserContainer extends ObjectContainer<User, UserResponse>
	                           implements UpdatableContainer<User, UserResponse, UserUpdateRequest>,
	                                      MultipleResponseContainer<User, UserResponse, UserResponseModifier> {

		private ObjectId userId;
		
		private UserRole userRole;
		
		private UserContainer(UserPrincipal currentUser, ObjectId userId) {
			this.currentUser = currentUser;
			this.userId = userId;
			
			load();
		}
		
		private UserContainer(UserPrincipal currentUser, User user) {
			this.currentUser = currentUser;
			this.userId = user.getId();
			this.object = user;
		}
		
		@Override 
		public void setObjectOwner() {
			
		}
		
		@Override
		public boolean delete() throws Exception {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public ObjectId getPrimaryId() {
			return userId;
		}

		private void loadUserRole() {
			Optional<UserRole> ur = roleRepository.findByDatabaseIdAndUserId(database.getId(), userId);
			if (ur.isPresent()) {
				userRole = ur.get();
			}
		}
		
		public UserRole getUserRole() {
			if (userRole == null) {
				loadUserRole();
			}
			
			return userRole;
		}
		
		public UserPrincipal asUserPrincipal() {
			return new UserPrincipal(object, getUserRole()); 
		}
	
		@Override
		public UserResponse asResponse(UserResponseModifier rm) {
			
	    	UserResponse response = new UserResponse();
	    	if (rm == null || rm.getId() != ResponseFieldType.IGNORE) {
	    		response.setId(object.getId().toString());
	    	}
	    	
    		response.setName(object.getName());
    		response.setIdentifier(object.getIdentifier());
	    	
	    	if (rm == null || rm.getEmail() != ResponseFieldType.IGNORE) {
	    		response.setEmail(object.getEmail());
	    	}
	    	
	    	if (getUserRole() != null) { 
	    		if (rm == null || rm.getRoles() != ResponseFieldType.IGNORE) {
	    			response.setRoles(userRole.getRole());
	    		}
	    	}

	    	if (rm != null) {
//	 	    	if (rm.getUserRole() == ResponseFieldType.EXPAND && getUserRole() != null) {
//		    		response.setRoles(userRole.getRole());
//		    	}
		    	
		    	if (rm.getDatasetCount() == ResponseFieldType.EXPAND) {
		    		response.setDatasetCount(datasetRepository.findByUserIdAndDatabaseId(object.getId(), database.getId()).size());
		    	}
		    	
		    	if (rm.getInOtherDatabases() == ResponseFieldType.EXPAND) {
		    		response.setInOtherDatabases(object.getDatabaseId().size() > 1);
		    	}
		    	
		    	if (rm.getAnnotationEditAcceptCount() == ResponseFieldType.EXPAND || rm.getAnnotationEditRejectCount() == ResponseFieldType.EXPAND || rm.getAnnotationEditAddCount() == ResponseFieldType.EXPAND ) {
		    		List<Dataset> datasets = datasetRepository.findByDatabaseId(database.getId());
				
		    		List<String> datasetUuids = datasets.stream().map(e -> e.getUuid()).collect(Collectors.toList());
				
		    		if (rm.getAnnotationEditAcceptCount() == ResponseFieldType.EXPAND) {
		    			response.setAnnotationEditAcceptCount(annotationEditRepository.countAcceptedByUserIdAndDatasetsUuid(object.getId(), datasetUuids));
		    		}
		    		
		    		if (rm.getAnnotationEditRejectCount() == ResponseFieldType.EXPAND) {
		    			response.setAnnotationEditRejectCount(annotationEditRepository.countRejectedByUserIdAndDatasetsUuid(object.getId(), datasetUuids));
		    		}
		    		
		    		if (rm.getAnnotationEditAddCount() == ResponseFieldType.EXPAND) {
		    			response.setAnnotationEditAddCount(annotationEditRepository.countAddedByUserIdAndDatasetsUuid(object.getId(), datasetUuids));
		    		}
		    	}
	    	}
	    	
	    	return response;
		}

		@Override
		public User update(UserUpdateRequest ur) throws Exception {
			
			if (ur.getPassword() != null) {
				if (!passwordEncoder.matches(ur.getOldPassword(), getObject().getbCryptPassword())) {
					throw new StateConflictException("Invalid password");
				} 
			}
			
			return update(iuc -> {
				User user = iuc.getObject();

				if (ur.getName() != null && ur.getName().trim().length() > 0) {
					user.setName(ur.getName());
				}
				if (ur.getEmail() != null && ur.getEmail().trim().length() > 0) {
					user.setEmail(ur.getEmail());
				}
				
				if (ur.getPassword() != null) {
					user.setbCryptPassword(passwordEncoder.encode(ur.getPassword()));
				}

			});
		}
		
		@Override
		public String getDescription() {
			return object.getName();
		}

		@Override
		protected String localSynchronizationString() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public DocumentRepository<User> getRepository() {
			return userRepository;
		}

		@Override
		public UserService getService() {
			return UserService.this;
		}

		@Override
		public TaskDescription getActiveTask(TaskType type) {
			// TODO Auto-generated method stub
			return null;
		}


	}
	
	@Override
	public User create(UserPrincipal currentUser, UserUpdateRequest ur) throws Exception {
	
		Optional<User> userOpt = userRepository.findByEmail(ur.getEmail());
		if (userOpt.isPresent()) {
			throw new StateConflictException("A user with that email already exists.");
		}

		User user = new User(database);
		user.setName(ur.getName());
		user.setEmail(ur.getEmail());
		user.setbCryptPassword(passwordEncoder.encode(ur.getPassword()));

		if (ur.getToken() != null) {
			synchronized (ur.getToken().intern()) {
				ActionToken token = tokenService.retrieveToken(ur.getToken());
	
				TokenDetails details = token.getScope();
				if (details == null || details.getRole() == null || !details.getEmail().equals(ur.getEmail())) {
					throw new Exception("Invalid token.");
				}
				
				final User newUser = userRepository.save(user);
	
				UserRole role = new UserRole(user.getId(), database.getId(), details.getRole());
				roleRepository.save(role);
	
				if (details.getProject() != null) {
					for (ObjectId projectId : details.getProject()) {
						ProjectContainer pc = projectService.getContainer(null, new SimpleObjectIdentifier(projectId));
						if (pc == null) {
							continue;
						}
						
						pc.update(ioc -> {
							ProjectDocument pdoc = ioc.getObject();
							
							pdoc.addMember(newUser);
						});
					}
				}
				
				tokenService.consume(token);
			}

		} else if (ur.getRole() != null) {
			user = userRepository.save(user);
		
			UserRole role = new UserRole(user.getId(), database.getId(), Arrays.asList(new UserRoleType[] { ur.getRole() }));
			roleRepository.save(role);
		
		}
		
//		for (String virtuoso : virtuosoConfigurations.values().stream().map(element -> element.getName()).collect(Collectors.toList())) {
//			tripleStore.addUserToAccessGraph(virtuoso, user.getUuid());
//		}
		
		return user;
	}

	public boolean addDatabaseToUser(String email, ObjectId databaseId) throws SQLException {
		
		Optional<User> user = userRepository.findByEmail(email);
		if (user.isPresent()) {
			return false;
		}

		User u = user.get();
		if (!u.getDatabaseId().contains(databaseId)) {
			u.addDatabaseId(databaseId);
			
			userRepository.save(u);
		}
		
		return true;
	}
	
//	public void addMissingDatabaseUsersToVirtuoso() throws Exception {
//		
////		logger.info("Adding missing users to Virtuosos");
//		
//		List<User> databaseUsers = new ArrayList<>();
//		
//		for (User user : userRepository.findAll()) {
//			for (ObjectId dbId : user.getDatabaseId()) {
//				if (database.getId().equals(dbId)) {
//					databaseUsers.add(user);
//					break;
//				}
//			}
//		}
//
//		String sparql = 
//				"SELECT ?user ?type WHERE { " + 
//		           "GRAPH <" + resourceVocabulary.getAccessGraphResource() + "> { " +
//			           "VALUES ?user { ";
//		
//		for (User user : databaseUsers) {
//			sparql += "<" + resourceVocabulary.getUserAsResource(user.getUuid()) + "> " ;
//		}
//		
//		
//		sparql += "} OPTIONAL { ?user a ?type } } } ";
//
//		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
////			System.out.println(vc.getName());
//			
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
//	
//				ResultSet rs = qe.execSelect();
//	
//				while (rs.hasNext()) {
//					QuerySolution sol = rs.next();
//					Resource user = sol.get("user").asResource();
//					Resource type = (Resource)sol.get("type");
//					
//					if (type == null) {
//						logger.info("Adding user " + user + " to " + vc.getName() );
//						tripleStore.addUserToAccessGraph(vc.getName(), resourceVocabulary.getUuidFromResourceUri(user.getURI()));
//					}
//				}
//			}
//		}
//	}
//	
//	public void addMissingDatabaseUserToVirtuoso(User user) throws Exception {
//		
//		boolean add = false;
//		for (ObjectId dbId : user.getDatabaseId()) {
//			if (database.getId().equals(dbId)) {
//				add = true;
//				break;
//			}
//		}
//		
//		if (!add) {
//			return;
//		}
//
//		String sparql = 
//				"SELECT ?user ?type WHERE { " + 
//		           "GRAPH <" + resourceVocabulary.getAccessGraphResource() + "> { " +
//			           "VALUES ?user { ";
//		
//		sparql += "<" + resourceVocabulary.getUserAsResource(user.getUuid()) + "> " ;
//		
//		
//		sparql += "} OPTIONAL { ?user a ?type } } } ";
//
//		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
////			System.out.println(vc.getName());
//			
//			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
//	
//				ResultSet rs = qe.execSelect();
//	
//				while (rs.hasNext()) {
//					QuerySolution sol = rs.next();
//					Resource suser = sol.get("user").asResource();
//					Resource type = (Resource)sol.get("type");
//					
//					if (type == null) {
//						logger.info("Adding user " + suser + " to " + vc.getName() );
//						tripleStore.addUserToAccessGraph(vc.getName(), resourceVocabulary.getUuidFromResourceUri(suser.getURI()));
//					}
//				}
//			}
//		}
//	}
//	
//	
//	public void transferUsersToNewConfiguration(String virtuoso) throws Exception {
//		for (User user : userRepository.findAll()) {
//			tripleStore.addUserToAccessGraph(virtuoso, user.getUuid());
//		}
//	}
//	
//    public void deleteAllUsers(String virtuoso) throws Exception {
//    	userRepository.deleteAll();
//    	
//    	tripleStore.resetAccessGraph(virtuoso);
//    }
    

	public User getUserByEmail(String email) {
		Optional<User> userOpt = userRepository.findByEmail(email);
		if (!userOpt.isPresent()) {
			return null;
		}

		User user = userOpt.get();
		return user;
	}

	public void changeUserPasswordFromToken(String password, String tk) throws Exception {
		synchronized (tk.intern()) {
			ActionToken token = tokenService.retrieveToken(tk);
	
			UserContainer oc = (UserContainer)apiUtils.exists(null, new SimpleObjectIdentifier(token.getScope().getUserId()), this);
	
			oc.update(ioc -> {
				User user = ioc.getObject();
	
				user.setbCryptPassword(passwordEncoder.encode(password));
			});
		
			tokenService.consume(token);
		}
	}

	@Override
	public UserContainer getContainer(UserPrincipal currentUser, User object) {
		UserContainer uc = new UserContainer(currentUser, object);
		
		if (uc.getObject() == null) {
			return null;
		} else {
			return uc;
		}
	}

	@Override
	public UserContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId) {
		UserContainer uc = new UserContainer(currentUser, ((SimpleObjectIdentifier)objId).getId());
		
		if (uc.getObject() == null) {
			return null;
		} else {
			return uc;
		}
	}

	@Override
	public String synchronizedString(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListPage<User> getAllByUser(ObjectId userId, Pageable page) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListPage<User> getAllByUser(ObjectId userId, UserLookupProperties lp, Pageable pg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListPage<User> getAll(UserLookupProperties lp, Pageable pg) {
		if (lp != null) {
			
			ListPage<User> res = new ListPage<>();
			
			List<UserRole> urList;
			if (pg == null) {
				urList = roleRepository.findByDatabaseIdAndRole(database.getId(), lp.getUserRoleType());
			} else {
				Page<UserRole> page = roleRepository.findByDatabaseIdAndRole(database.getId(), lp.getUserRoleType(), pg);
				urList = page.getContent();
				
				res.setPagination(Pagination.fromPage(page));
			}
			
			List<User> uList = new ArrayList<>();
			for (UserRole ur : urList) {
				uList.add(userRepository.findById(ur.getUserId()).get());
			}
			
			res.setList(uList);
			
			return res;
			
			
		} else {
			if (pg == null) {
				return ListPage.create(userRepository.findByDatabaseId(database.getId()));
			} else {
				return ListPage.create(userRepository.findByDatabaseId(database.getId(), pg));
			}
		}
	}

	@Override
	public UserLookupProperties createLookupProperties() {
		return new UserLookupProperties();
	}


}
