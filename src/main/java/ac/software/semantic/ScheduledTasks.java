package ac.software.semantic;

import ac.software.semantic.model.FileSystemConfiguration;
import ac.software.semantic.model.PagedAnnotationValidationPageLocks;
import ac.software.semantic.model.PublicTask;
import ac.software.semantic.repository.core.PagedAnnotationValidationPageLocksRepository;
import ac.software.semantic.repository.core.PublicTaskRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Date;
import java.util.List;

@Component
public class ScheduledTasks {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTasks.class);

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    @Autowired
    private PagedAnnotationValidationPageLocksRepository locksRepository;

	@Autowired
	private PublicTaskRepository publicTaskRepository;

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
    
    @Scheduled(fixedRate = 300000)
	public void cleanPublicTasks() {
    	
    	long ms = System.currentTimeMillis() - 7*24*60*60*1000; // a week ago
    	
    	Date date = new Date(ms);
    	
    	for (PublicTask pt : publicTaskRepository.findByCompletedAtBeforeAndFileSystemConfigurationId(date, fileSystemConfiguration.getId())) {
    		String output = (String)pt.getParameters().get("output");
    		
    		if (output != null) {
    			String file = fileSystemConfiguration.getPublicFolder() + File.separatorChar + pt.getUuid() + "." + output.toLowerCase();
    			File f = new File(file);
    			if (f.exists()) {
    				f.delete();
    			}
    		}
    		
    		publicTaskRepository.delete(pt);
    		
    	}
	}
	
}
