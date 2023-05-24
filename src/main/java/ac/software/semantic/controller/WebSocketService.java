package ac.software.semantic.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import ac.software.semantic.config.WebSocketConfig;
import ac.software.semantic.model.SseMessageObject;
import ac.software.semantic.model.constants.NotificationChannel;
import ac.software.semantic.security.UserPrincipal;

@Service
public class WebSocketService {
	
	@Autowired
	private SimpMessagingTemplate simpMessagingTemplate;

    public void send(NotificationChannel channel, UserPrincipal currentUser, SseMessageObject no) {

		try {
			simpMessagingTemplate.convertAndSend("/" + WebSocketConfig.monitorChannel + "/" + currentUser.getId() + "/" + channel.toString(), no);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
}
