package ac.software.semantic.security;

import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.User;
import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.constants.UserRoleType;
import ac.software.semantic.repository.UserRepository;
import ac.software.semantic.repository.UserRoleRepository;
import ac.software.semantic.service.UserService;

@Service
public class CustomUserDetailsService implements UserDetailsService {

	private Logger logger = LoggerFactory.getLogger(CustomUserDetailsService.class);
	
    @Autowired
    private UserRepository userRepository;

	@Autowired
	private UserRoleRepository roleRepository;
	
    @Autowired
    private UserService userService;
    
    @Autowired
    @Qualifier("database")
    private Database database;

    
//    public UserDetails loadUserByUsername(String email) throws UsernameNotFoundException {
//        //Let's split the email to get both values
//        String[] usernameAndCustomToken = StringUtils.split(email, String.valueOf(Character.LINE_SEPARATOR));
//
//        //if the String arrays is empty or size is not equal to 2, let's throw exception
//        if (Objects.isNull(usernameAndCustomToken) || usernameAndCustomToken.length != 2) {
//            throw new UsernameNotFoundException("User not found");
//        }
//        final String userName = usernameAndCustomToken[0];
//        final String customToken = usernameAndCustomToken[1]; // use it based on your requirement
//        final UserEntity customer = userRepository.findByEmail(userName);
//        if (customer == null) {
//            throw new UsernameNotFoundException(email);
//        }
//        boolean enabled = !customer.isAccountVerified(); // we can use this in case we want to activate account after customer verified the account
//        UserDetails user = User.withUsername(customer.getEmail())
//            .password(customer.getPassword())
//            .disabled(customer.isLoginDisabled())
//            .authorities(getAuthorities(customer)).build();
//
//        return user;
//    }
    
    @Override
    @Transactional
    public UserDetails loadUserByUsername(String email) throws UsernameNotFoundException {
    	String[] elements = email.split("~~~");
    	
    	String role = null;
    	
    	if (elements.length > 1) {
    		email = elements[0];
    		role = elements[1];
    	}
    	
        // Let people login with either username or email
        Optional<User> userOpt = userRepository.findByEmail(email);
        
        User user = null;
        
        if (!userOpt.isPresent()) {
        	throw new UsernameNotFoundException("User not found with username or email : " + email);
        } else if (!userOpt.get().getDatabaseId().contains(database.getId())) {
        	user = userOpt.get();
//        	throw new UsernameNotFoundException("User has not access to current database");
        	try {
        		user = registerUserInDatabase(user);
        	} catch (Exception e) {
        		throw new UsernameNotFoundException("User has not access to current database");
        	}
        } else {
        	user = userOpt.get();
        }

        UserPrincipal principal = new UserPrincipal(user);
        if (role != null) {
        	principal.setType(UserRoleType.valueOf(role));
        }
        
        return principal;
        
//        return UserPrincipal.create(user.get());
    }

    // This method is used by JWTAuthenticationFilter
    @Transactional
    public UserDetails loadUserById(String id) {
        Optional<User> userOpt = userRepository.findById(id);
       
        User user = null;
        
        if (!userOpt.isPresent()) {
        	throw new UsernameNotFoundException("User not found with id : " + id);
        } else if (!userOpt.get().getDatabaseId().contains(database.getId())) {
        	user = userOpt.get();
//        	throw new UsernameNotFoundException("User has not access to current database");
        	try {
        		user = registerUserInDatabase(user);
        	} catch (Exception e) {
        		throw new UsernameNotFoundException("User has not access to current database");
        	}
        	
        } else {
        	user = userOpt.get();
        }
        
        return new UserPrincipal(user);
    }
    
    private User registerUserInDatabase(User user) throws Exception {
    	if (!user.getDatabaseId().contains(database.getId())) {
    		
    		logger.info("Granting access for user " + user.getEmail() + " to database " + database.getName());
    		user.getDatabaseId().add(database.getId()); 
    		
    		userRepository.save(user);
    		
    		UserRole ur = new UserRole();
    		ur.setUserId(user.getId());
    		ur.setDatabaseId(database.getId());
    		ur.setRole(Arrays.asList(new UserRoleType[] { UserRoleType.VALIDATOR}));
    		
    		roleRepository.save(ur);
    		
    		userService.addMissingDatabaseUserToVirtuoso(user);
    	}
    	
		return user;

    }
}