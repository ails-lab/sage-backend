package ac.software.semantic.service;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import ac.software.semantic.controller.APIUserController;
import ac.software.semantic.model.*;
import ac.software.semantic.payload.ChangeUserDetailsRequest;
import ac.software.semantic.payload.UserDetails;
import ac.software.semantic.repository.AccessRepository;
import ac.software.semantic.repository.TokenPasswordResetRepository;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import ac.software.semantic.payload.EditorItem;
import ac.software.semantic.payload.NewUserSummary;
import ac.software.semantic.repository.UserRepository;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;

import org.springframework.beans.factory.annotation.Value;

@Service
public class UserService {

	Logger logger = LoggerFactory.getLogger(UserService.class);
	
	@Autowired
	private UserRepository userRepository;

	@Autowired
	private PasswordEncoder passwordEncoder;

	@Autowired
	private DatabaseService databaseService;

	@Autowired
	private AccessRepository accessRepository;

	@Autowired
	TokenPasswordResetRepository tokenPasswordResetRepository;

	@Autowired
	VirtuosoJDBC virtuosoJDBC;
	
    @Autowired
    @Qualifier("database")
    private Database database;

	@Value("${database.name}")
	private String databaseName;
	
	@Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfigurations;
	
	public NewUserSummary createUser(String email, String password, String name, UserType type) throws SQLException {
		
		Optional<User> user = userRepository.findByEmail(email);
		if (user.isPresent()) {
			return null;
		}

		String uuid = UUID.randomUUID().toString();
		ObjectId databaseId = databaseService.findDatabase(databaseName).getId();

		User newUser  = new User(email, passwordEncoder.encode(password), uuid, name, type);
		newUser.addDatabaseId(databaseId);
		userRepository.save(newUser);
		
		for (String virtuoso : virtuosoConfigurations.values().stream().map(element -> element.getName()).collect(Collectors.toList())) {
			virtuosoJDBC.addUserToAccessGraph(virtuoso, uuid);
		}
		
		return new NewUserSummary(newUser);
	}

	public List<EditorItem> getPublicEditors() throws SQLException {

		List<User> publicEditors = userRepository.findByTypeAndIsPublic(UserType.EDITOR, true);
		List<EditorItem> response = publicEditors.stream()
			.map(editor -> new EditorItem(editor))
			.collect(Collectors.toList());
		return response;
	}
	
	public List<NewUserSummary> getValidatorsOfEditor(String id) throws SQLException {
		Optional<User> loggedInUser = userRepository.findById(id);
		List<User> validators = new ArrayList<User>();
		if (loggedInUser.isPresent()) {
			for (ObjectId validatorId : loggedInUser.get().getValidatorList()) {
				Optional<User> retreivedUser = userRepository.findById(validatorId.toString());
				retreivedUser.ifPresent(val -> validators.add(val));
			}
			List<NewUserSummary> response = validators.stream()
				.map(validator -> new NewUserSummary(validator))
				.collect(Collectors.toList());
			return response;
		}
		else {
			return null;
		}
	}

	public boolean addValidatorToEditor(String editorId, String currentUserId) throws SQLException {
		Optional<User> editorOpt = userRepository.findById(editorId);
		if (editorOpt.isPresent()) {
			User editor = editorOpt.get();
			List<String> validators = editor.getValidatorList().stream()
					.map(validatorId -> validatorId.toString())
					.collect(Collectors.toList());
			if (validators.contains(currentUserId)) {
				return false;
			}
			else {
				editor.addValidator(new ObjectId(currentUserId));
				userRepository.save(editor);
				return true;
			}
		}
		else {
			return false;
		}
	}

	public boolean removeValidatorFromEditor(String validatorId, String currentUserId) throws SQLException {
		Optional<User> editorOpt = userRepository.findById(currentUserId);
		if (editorOpt.isPresent()) {
			User editor = editorOpt.get();
			List<ObjectId> validators = editor.getValidatorList();
			boolean success = validators.remove(new ObjectId(validatorId));
			editor.setValidatorList(validators);
			List<Access> accessEntries = accessRepository.findByCreatorIdAndUserIdAndAccessType(new ObjectId(currentUserId), new ObjectId(validatorId), AccessType.VALIDATOR);
			for (Access acc : accessEntries) {
				accessRepository.delete(acc);
			}
			userRepository.save(editor);
			return true;
		}
		else {
			return false;
		}
	}

	public List<EditorItem> getEditorsOfValidator(String validatorId) throws SQLException {
		List<ObjectId> validator = new ArrayList<>();
		validator.add(new ObjectId(validatorId));
		List<User> editors = userRepository.findByValidatorListIn(validator);
		List<EditorItem> response = editors.stream()
				.map(editor -> new EditorItem(editor))
				.collect(Collectors.toList());
		return response;
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
	
	public void addMissingDatabaseUsersToVirtuoso() throws SQLException {
		
//		logger.info("Adding missing users to Virtuosos");
		
		List<User> databaseUsers = new ArrayList<>();
		
		for (User user : userRepository.findAll()) {
			for (ObjectId dbId : user.getDatabaseId()) {
				if (database.getId().equals(dbId)) {
					databaseUsers.add(user);
					break;
				}
			}
		}

		String sparql = 
				"SELECT ?user ?type WHERE { " + 
		           "GRAPH <" + SEMAVocabulary.accessGraph.toString() + "> { " +
			           "VALUES ?user { ";
		
		for (User user : databaseUsers) {
			sparql += "<" + SEMAVocabulary.getUser(user.getUuid()) + "> " ;
		}
		
		
		sparql += "} OPTIONAL { ?user a ?type } } } ";

		for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) {
//			System.out.println(vc.getName());
			
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
	
				ResultSet rs = qe.execSelect();
	
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					Resource user = sol.get("user").asResource();
					Resource type = (Resource)sol.get("type");
					
					if (type == null) {
						logger.info("Adding user " + user + " to " + vc.getName() );
						virtuosoJDBC.addUserToAccessGraph(vc.getName(), SEMAVocabulary.getId(user.getURI()));
					}
				}
			}
		}
	}
	
	
	public void transferUsersToNewConfiguration(String virtuoso) throws SQLException {
		for (User user : userRepository.findAll()) {
			virtuosoJDBC.addUserToAccessGraph(virtuoso, user.getUuid());
		}
	}
	
    public void deleteAllUsers(String virtuoso) throws SQLException {
    	userRepository.deleteAll();
    	
    	virtuosoJDBC.resetAccessGraph(virtuoso);
    }

    public UserDetails getUserById(String userId) throws SQLException {
		Optional<User> userOpt = userRepository.findById(userId);
		if (!userOpt.isPresent()) {
			return null;
		}

		User user = userOpt.get();
		UserDetails response = new UserDetails(user);
		return response;
	}

	public User getUserByEmail(String email) {
		Optional<User> userOpt = userRepository.findByEmail(email);
		if (!userOpt.isPresent()) {
			return null;
		}

		User user = userOpt.get();
		return user;
	}

    public UserDetails updateUser(String userId, APIUserController.UserDetailsUpdateOptions target, ChangeUserDetailsRequest request) {
		Optional<User> userOpt = userRepository.findById(userId);
		if (!userOpt.isPresent()) {
			return null;
		}

		User user = userOpt.get();

		switch (target) {
			case EMAIL:
				user.setEmail(request.getValue());
				break;
			case PUBLIC:
				if (request.getValue() == "true") {
					user.setPublic(true);
				} else {
					user.setPublic(false);
				}
				break;
			case NAME:
				user.setName(request.getValue());
				break;
			case JOBDESCRIPTION:
				user.setJobDescription(request.getValue());
				break;
			case PASSWORD:
				if (passwordEncoder.matches(request.getOldPassword(), user.getbCryptPassword())) {
					user.setbCryptPassword(passwordEncoder.encode(request.getNewPassword()));
				}
				else {
					return null;
				}
				break;
		}

		userRepository.save(user);
		return new UserDetails(user);

	}

	public boolean changeUserPasswordFromToken(String password, String token) {
		Optional<TokenPasswordReset> tokenOpt = tokenPasswordResetRepository.findByToken(token);
		if (!tokenOpt.isPresent()) {
			return false;
		}

		TokenPasswordReset tokenModel = tokenOpt.get();
		Calendar cal = Calendar.getInstance();
		if (cal.getTime().before(tokenModel.getExpiryDate())) {
			User u = tokenModel.getUser();
			u.setbCryptPassword(passwordEncoder.encode(password));
			userRepository.save(u);
			tokenPasswordResetRepository.delete(tokenModel);
			return true;
		}
		else {
			return false;
		}
	}

}
