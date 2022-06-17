package ac.software.semantic.security;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.User;
import ac.software.semantic.repository.UserRepository;

@Service
public class CustomUserDetailsService implements UserDetailsService {

    @Autowired
    UserRepository userRepository;

    @Autowired
    @Qualifier("database")
    private Database database;

    @Override
    @Transactional
    public UserDetails loadUserByUsername(String email)
            throws UsernameNotFoundException {
        // Let people login with either username or email
        Optional<User> user = userRepository.findByEmail(email);
        
        if (!user.isPresent()) {
        	throw new UsernameNotFoundException("User not found with username or email : " + email);
        } else if (!user.get().getDatabaseId().contains(database.getId())) {
        	throw new UsernameNotFoundException("User has not access to current database");
        }

        return UserPrincipal.create(user.get());
    }

    // This method is used by JWTAuthenticationFilter
    @Transactional
    public UserDetails loadUserById(String id) {
        Optional<User> user = userRepository.findById(id);
       
        if (!user.isPresent()) {
        	throw new UsernameNotFoundException("User not found with id : " + id);
        } else if (!user.get().getDatabaseId().contains(database.getId())) {
        	throw new UsernameNotFoundException("User has not access to current database");
        }
        
        return UserPrincipal.create(user.get());
    }
}