package ac.software.semantic.service;

import java.util.Date;
import java.util.Optional;
import org.bson.types.ObjectId;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.UserSession;
import ac.software.semantic.repository.core.UserSessionRepository;


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
    
	public class LoginInfo {
		private Date lastLogin;
		private UserSession userSession;
		
		private LoginInfo(Date lastLogin, UserSession userSession) {
			this.lastLogin = lastLogin;
			this.userSession = userSession;
		}

		public Date getLastLogin() {
			return lastLogin;
		}

		public void setLastLogin(Date lastLogin) {
			this.lastLogin = lastLogin;
		}

		public UserSession getUserSession() {
			return userSession;
		}

		public void setUserSession(UserSession userSession) {
			this.userSession = userSession;
		}
	}
	
	public LoginInfo login(ObjectId userId, String ip) {
		
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

		us = userSessionRepository.save(us);
		
		return new LoginInfo(lastLogin, us);
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