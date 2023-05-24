package ac.software.semantic.service;

import java.util.Date;
import java.util.Optional;
import org.bson.types.ObjectId;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.UserSession;
import ac.software.semantic.repository.UserSessionRepository;


@Service
public class UserSessionService {

    @Autowired
    @Qualifier("database")
    private Database database;
    
    @Autowired
    private UserSessionRepository userSessionRepository;

	public Optional<Date> getLastLogin(ObjectId userId) {
		
		Optional<UserSession> usOpt = userSessionRepository.findByUserIdAndDatabaseId(userId, database.getId());
		
		if (usOpt.isPresent()) {
			UserSession us = usOpt.get();
			return Optional.of(us.getLogin());
		} else {
			return Optional.empty();
		}
	}
    
	public Date login(ObjectId userId, String ip) {
		
		Optional<UserSession> usOpt = userSessionRepository.findByUserIdAndDatabaseId(userId, database.getId());
		
		UserSession us;
		Date lastLogin = null;
		if (usOpt.isPresent()) {
			us = usOpt.get();
			lastLogin = us.getLogin();
			us.setLogout(null);
		} else {
			us = new UserSession();
			us.setUserId(userId);
			us.setDatabaseId(database.getId());
		}
		
		us.setLogin(new Date());
		us.setAddress(ip);


		userSessionRepository.save(us);
		
		return lastLogin;
	}
	
	public boolean logout(ObjectId userId) {
		
		Optional<UserSession> usOpt = userSessionRepository.findByUserIdAndDatabaseId(userId, database.getId());
		
		if (usOpt.isPresent()) {
			UserSession us = usOpt.get();
			us.setLogout(new Date());
			
			userSessionRepository.save(us);
			
			return true;
		} else {
			return false;
		}

		
	}

}