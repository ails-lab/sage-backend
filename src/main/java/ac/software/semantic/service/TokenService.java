package ac.software.semantic.service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.DatedDocument;
import ac.software.semantic.model.TokenDetails;
import ac.software.semantic.model.ActionToken;
import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.type.TokenState;
import ac.software.semantic.model.constants.type.TokenType;
import ac.software.semantic.payload.request.InviteUserRequest;
import ac.software.semantic.repository.core.TokenRepository;
import ac.software.semantic.service.UserService.UserContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;

@Service
public class TokenService {

    @Autowired
    private TokenRepository tokenRepository;

    @Autowired
    private JavaMailSender javaMailSender;

    @Autowired
    @Qualifier("database")
	private Database database;
    
    @Value("${resetTokenExpiration}")
    private int resetTokenExpiration;

//    @Value("app.mode")
//    private String applicationDeployMode;
    @Value("${backend.server}")
    private String server;

    public ActionToken createPasswordResetToken(User user) {
    	ActionToken token = new ActionToken(database);
    	token.setCreatedAt(new Date());
    	token.setType(TokenType.PASSWORD_RESET);
    	token.setExpiration(resetTokenExpiration);
    	
    	TokenDetails action = new TokenDetails();
    	action.setEmail(user.getEmail());
    	action.setUserId(user.getUserId());
    	
    	token.setScope(action);
    	
        tokenRepository.save(token);
        
        return token;
    }

    public ActionToken createSignUpToken(UserContainer uc, InviteUserRequest iur) {
    	ActionToken token = new ActionToken(database);
    	token.setUserId(uc.getObject().getId());
    	token.setCreatedAt(new Date());
    	token.setType(TokenType.SIGN_UP);
    	token.setExpiration(resetTokenExpiration);
    	
    	TokenDetails action = new TokenDetails();
    	action.setRole(iur.getRole());
    	action.setEmail(iur.getEmail());
    	
    	if (iur.getProjectId() != null) {
    		action.setProject(iur.getProjectId());
    	}
    	
    	token.setScope(action);
    	
        tokenRepository.save(token);
        
        return token;

    }

    public ActionToken retrieveToken(String tk) throws Exception {
		Optional<ActionToken> tokenOpt = tokenRepository.findByToken(tk);
		if (!tokenOpt.isPresent()) {
			throw new Exception("The token is invalid.");
		}
		
		ActionToken token = tokenOpt.get();
		if (token.hasExpired() || token.getState() == TokenState.USED || !token.getDatabaseId().equals(database.getId())) {
			throw new Exception("The token is invalid.");
		}
		
		return token;
    }
    
    public void consume(ActionToken token) {
		token.setState(TokenState.USED);
		token.setUsedAt(new Date());
		
		tokenRepository.save(token);
    }

    public void sendMail(String email, ActionToken token) {
    	
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom("SAGE Platform <sage-do-not-reply@ails.ece.ntua.gr>");
        message.setTo(email);
        
        if (token.getType() == TokenType.PASSWORD_RESET) {
        	message.setSubject("SAGE Account Password Recovery");
        
        	String url = server + "/";
        
        	message.setText("Hello from SAGE / " + database.getLabel() + ",\n" +
                        "It seems that you asked for a password reset.\n" +
                        "To reset your password use the following link:\n\n" +
                        url + "reset-password?token=" + URLEncoder.encode(token.getToken()) + "\n\n" +
                        "The link will expire in 24 hours.\n\n" +
                        "If you didn't requested to reset your password, ignore this message.\n\n" +
                        "Please do not reply to this address.\n\n" +
                        "SAGE Platform\n");
        
        	javaMailSender.send(message);
        	
        } else if (token.getType() == TokenType.SIGN_UP) {
        	message.setSubject("Invitation to join SAGE");
            
        	String url = server + "/";
        
        	message.setText("Hello from SAGE / " + database.getLabel() + ",\n" +
                        "You have been invited to join SAGE.\n" +
                        "To join SAGE use the following link:\n\n" +
                        url + "signup?token=" + URLEncoder.encode(token.getToken()) + "&email=" + URLEncoder.encode(email) + "\n\n" +
                        "The link will expire in 24 hours.\n\n" +
                        "Please do not reply to this address.\n\n" +
                        "SAGE Platform\n");
        
        	javaMailSender.send(message);
        }
    }
}
