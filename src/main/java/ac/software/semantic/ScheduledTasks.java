package ac.software.semantic;

import ac.software.semantic.model.PagedAnnotationValidationPageLocks;
import ac.software.semantic.repository.PagedAnnotationValidationPageLocksRepository;
//import ac.software.semantic.repository.SentSseEventRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

@Component
public class ScheduledTasks {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTasks.class);

    @Autowired
    private PagedAnnotationValidationPageLocksRepository locksRepository;

    @Value("${lock.max.age.minutes}")
    private int maxLockAge;

    // Run every 5 minutes
    @Scheduled(fixedRate = 300000)
    public void checkAndRemoveLocks(){
        Date limit = new Date(System.currentTimeMillis() - maxLockAge * 1000 * 60);
//        log.info("Checking for stale locks to remove..");
        List<PagedAnnotationValidationPageLocks> list = locksRepository.findByCreatedAtLessThan(limit);
        for (PagedAnnotationValidationPageLocks lock : list) {
            locksRepository.delete(lock);
        }
    }

    
//    @Autowired
//    private SentSseEventRepository sseRepository;
//    
//	@Scheduled(fixedRate = 300000) // 5 minutes
////    @Scheduled(fixedRate = 5000) // 
//    @Transactional
//	public void deleleOldSseEvents() {
////    	System.out.println("DELETING SSE");
//		sseRepository.deleteByTimestampLessThan(new Date().getTime() - 300000); 
//	}
	
}
