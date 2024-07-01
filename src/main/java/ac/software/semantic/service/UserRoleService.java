package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.constants.type.UserRoleType;
import ac.software.semantic.repository.core.UserRoleRepository;

@Service
public class UserRoleService {

	Logger logger = LoggerFactory.getLogger(UserRoleService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private UserRoleRepository roleRepository;
	
	public List<UserRoleType> getLoginRolesForUser(ObjectId userId) {
		Optional<UserRole> roles = roleRepository.findByDatabaseIdAndUserId(database.getId(), userId);
		
		List<UserRoleType> res;
		
		if (roles.isPresent()) {
			res = roles.get().getRole();
		} else {
			res = new ArrayList<>();
		}
		
		
		return res;
	}

}
