package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import ac.software.semantic.model.NotificationObject;



@RestController
@RequestMapping("/api/server")
public class SSEController implements ApplicationListener<SseApplicationEvent> {

    final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    
    @GetMapping(value = "/sse")
    public ResponseEntity<SseEmitter> getSseEmitter() {
        // timeout after 5 minutes
    	final SseEmitter emitter = new SseEmitter();
    	emitters.add(emitter);
		emitter.onCompletion(() -> emitters.remove(emitter));
		emitter.onTimeout(() -> emitters.remove(emitter));
		
		return new ResponseEntity<>(emitter, HttpStatus.OK);
    }
    
    @Override
    public void onApplicationEvent(SseApplicationEvent event) {
    	List<SseEmitter> deadEmitters = new ArrayList<>();
        this.emitters.forEach(emitter -> {
          try {
            emitter.send(event.getSseEventBuilder());

            // close connnection, browser automatically reconnects
            // emitter.complete();

            // SseEventBuilder builder = SseEmitter.event().name("second").data("1");
            // SseEventBuilder builder =
            // SseEmitter.event().reconnectTime(10_000L).data(memoryInfo).id("1");
            // emitter.send(builder);
          }
          catch (Exception e) {
            deadEmitters.add(emitter);
          }
        });

        this.emitters.removeAll(deadEmitters);
    } 
    
    
    public static void send(String channel, ApplicationEventPublisher applicationEventPublisher, Object source, Object no) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	    mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
    	
		try {
			SseEventBuilder sse = SseEmitter.event().name(channel).data(mapper.writeValueAsBytes(no));
	
			applicationEventPublisher.publishEvent(new SseApplicationEvent(source, sse));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
}
