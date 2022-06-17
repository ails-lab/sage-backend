package ac.software.semantic.service;

import ac.software.semantic.model.TokenPasswordReset;
import ac.software.semantic.model.User;
import ac.software.semantic.repository.TokenPasswordResetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

@Service
public class TokenPasswordResetService {

    @Autowired
    private TokenPasswordResetRepository tokenPasswordResetRepository;

    @Autowired
    private JavaMailSender javaMailSender;

    @Value("${resetTokenExpiration}")
    private int resetPasswordTokenExpiration;

    @Value("app.mode")
    private String applicationDeployMode;

    private Date calculateExpiryDate() {
        System.out.println(resetPasswordTokenExpiration);
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(new Date().getTime());
        cal.add(Calendar.MINUTE, resetPasswordTokenExpiration);
        return new Date(cal.getTime().getTime());
    }

    public String createToken(User user) {
        String token = UUID.randomUUID().toString();
        TokenPasswordReset resetToken = new TokenPasswordReset(token, user);
        resetToken.setExpiryDate(calculateExpiryDate());
        tokenPasswordResetRepository.save(resetToken);
        return token;
    }

    public void sendMail(User user, String token) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom("sage-do-not-reply@ails.ece.ntua.gr");
        message.setTo(user.getEmail());
        message.setSubject("SAGE Account Password Recovery");
        String url = "";
        if (applicationDeployMode == "development") {
            url = "https://europeana-semantic-dev.ails.ece.ntua.gr/";
        }
        else {
            url = "https://europeana-semantic.ails.ece.ntua.gr/";
        }
        message.setText("Hello from SAGE Europeana,\n" +
                        "It seems that you asked for a password reset.\n" +
                        "Use the following link to reset your password:\n\n" +
                        url + "resetPassword?token="+token+"\n\n" +
                        "If it wasn't you, ignore this message.\n" +
                        "Please do not reply to this address.\n\n" +
                        "SAGE Platform\n");
        javaMailSender.send(message);
    }
}
