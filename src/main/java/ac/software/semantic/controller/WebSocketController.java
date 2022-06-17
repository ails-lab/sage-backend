//package ac.software.semantic.controller;
//
//import java.security.Principal;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
//import org.springframework.messaging.handler.annotation.MessageMapping;
//import org.springframework.messaging.handler.annotation.Payload;
//import org.springframework.messaging.simp.SimpMessageSendingOperations;
//import org.springframework.messaging.simp.SimpMessagingTemplate;
//import org.springframework.messaging.simp.annotation.SendToUser;
//import org.springframework.stereotype.Controller;
//
////@Controller
////public class WebSocketController {
////   
////	@Autowired
////    SimpMessagingTemplate simpMessagingTemplate;
////
////    @MessageMapping("/processrequest")
////    void runWebSocket( RequestData requestData ) {
////        new Thread(new RunProcess(requestData)).start();
////    }
////
////    private class RunProcess implements Runnable {
////        private RequestData requestData;
////
////        RunProcess(RequestData requestData) {
////            this.requestData = requestData;
////        }
////
////        public void run() {
////            simpMessagingTemplate.convertAndSend("/topic/response", requestData.getString1());
////            simpMessagingTemplate.convertAndSend("/topic/response", requestData.getString2());
////            simpMessagingTemplate.convertAndSend("/topic/response", "A third response via websocket");
////        }
////    }
//	
//@Controller
//public class WebSocketController {
//
//	@MessageMapping("/message")
//	@SendToUser("/queue/reply")
//	public String processMessageFromClient() throws Exception {
//		String name = "myname";
//		//messagingTemplate.convertAndSendToUser(principal.getName(), "/queue/reply", name);
//		return name;
//	}
//	
//	@MessageExceptionHandler
//    @SendToUser("/queue/errors")
//    public String handleException(Throwable exception) {
//        return exception.getMessage();
//    }
//
//}	
