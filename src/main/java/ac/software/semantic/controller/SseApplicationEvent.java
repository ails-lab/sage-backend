package ac.software.semantic.controller;

import org.springframework.context.ApplicationEvent;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

public class SseApplicationEvent extends ApplicationEvent  {
    private SseEventBuilder sse;
	 
    public SseApplicationEvent(Object source, SseEventBuilder sse) {
    	super(source);
        this.sse = sse;
    }
    
    public SseEventBuilder getSseEventBuilder() {
        return sse;
    }
}
