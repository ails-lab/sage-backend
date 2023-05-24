package ac.software.semantic.service;

import ac.software.semantic.controller.APIAnnotationEditGroupController.AnnotationValidationRequest;
import ac.software.semantic.model.PagedAnnotationValidation;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.PagedAnnotationValidationPageLocks;
import ac.software.semantic.payload.PagedAnnotationValidatationDataResponse;
import ac.software.semantic.repository.PagedAnnotationValidationPageLocksRepository;
import ac.software.semantic.repository.PagedAnnotationValidationRepository;
import ac.software.semantic.repository.PagedAnnotationValidationPageRepository;
import ac.software.semantic.security.UserPrincipal;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;

@Service
public class PagedAnnotationValidationPageLocksService {

    Logger logger = LoggerFactory.getLogger(PagedAnnotationValidationPageLocksService.class);

    @Autowired
    private PagedAnnotationValidationPageLocksRepository locksRepository;

    @Autowired
    private PagedAnnotationValidationRepository pavRepository;

    @Autowired
    private PagedAnnotationValidationPageRepository pavpRepository;

    //throws org.springframework.dao.DuplicateKeyException if index already in Database
//    public boolean obtainLock(String userId, String pagedAnnotationValidationId, int page, AnnotationValidationRequest mode) {
//        Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pagedAnnotationValidationId));
//        if (!pavOpt.isPresent()) {
//            return false;
//        }
//        PagedAnnotationValidation pav = pavOpt.get();
//
//        PagedAnnotationValidationPageLocks lock = new PagedAnnotationValidationPageLocks();
//        lock.setPagedAnnotationValidationId(new ObjectId(pagedAnnotationValidationId));
//        lock.setAnnotationEditGroupId(pav.getAnnotationEditGroupId());
//        lock.setUserId(new ObjectId(userId));
//        lock.setPage(page);
//        lock.setMode(mode);
//        lock.setCreatedAt(new Date(System.currentTimeMillis()));
//        boolean result = true;
//        try {
//            locksRepository.insert(lock);
//        }
//        catch(Exception e) {
//            result =  false;
//        }
//        return result;
//    }

    public ObjectId obtainLock(String userId, String pagedAnnotationValidationId, int page, AnnotationValidationRequest mode) {
        Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pagedAnnotationValidationId));
        if (!pavOpt.isPresent()) {
            return null;
        }
        PagedAnnotationValidation pav = pavOpt.get();

        PagedAnnotationValidationPageLocks lock = new PagedAnnotationValidationPageLocks();
        lock.setPagedAnnotationValidationId(new ObjectId(pagedAnnotationValidationId));
        lock.setAnnotationEditGroupId(pav.getAnnotationEditGroupId());
        lock.setUserId(new ObjectId(userId));
        lock.setPage(page);
        lock.setMode(mode);
        lock.setCreatedAt(new Date(System.currentTimeMillis()));
        boolean result = true;
        try {
            locksRepository.insert(lock);
        }
        catch(Exception e) {
            return null;
        }
        return lock.getId();
    }

    public boolean removeLock(String userId, String pavId, int page, AnnotationValidationRequest mode) {
        Optional<PagedAnnotationValidation> pavOpt = pavRepository.findById(new ObjectId(pavId));
        if (!pavOpt.isPresent()) {
            return false;
        }
        PagedAnnotationValidation pav = pavOpt.get();

        Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findByPagedAnnotationValidationIdAndModeAndPage(new ObjectId(pavId), mode, page);
        if (pavpOpt.isPresent()) {
            PagedAnnotationValidationPage pavp = pavpOpt.get();
            pavp.setAssigned(false);
            pavpRepository.save(pavp);
        }

        Optional<PagedAnnotationValidationPageLocks> lockOpt = locksRepository.findByPagedAnnotationValidationIdAndPageAndModeAndUserId(new ObjectId(pavId), page, mode, new ObjectId(userId));
        if (lockOpt.isPresent()) {
            locksRepository.delete(lockOpt.get());
            return true;
        }
        return false;
    }

    /*
		Check if currentUser has a lock acquired. If yes, remove them.
	 */
    public boolean checkForLockAndRemove(UserPrincipal currentUser) {
        Optional<PagedAnnotationValidationPageLocks> lockOpt = locksRepository.findByUserId(new ObjectId(currentUser.getId()));
        if (!lockOpt.isPresent()) {
            return true;
        }

        PagedAnnotationValidationPageLocks lock  = lockOpt.get();

        // we also want to update the released page (if it exists)!
        Optional<PagedAnnotationValidationPage> pavpOpt = pavpRepository.findByPagedAnnotationValidationIdAndModeAndPage(lock.getPagedAnnotationValidationId(), lock.getMode(), lock.getPage());
        if (pavpOpt.isPresent()) {
            PagedAnnotationValidationPage pavp = pavpOpt.get();
            pavp.setAssigned(false);
            try {
                pavpRepository.save(pavp);
            }
            catch(Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        try {
            locksRepository.delete(lock);
        }
        catch(Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // This is called on page commit time. Check if i have a lock (should only have one at a time) and
    // if that lock is for that page.
    public boolean checkForLock(UserPrincipal currentUser, String pavId, String lockId, AnnotationValidationRequest mode, int page) {
        Optional<PagedAnnotationValidationPageLocks> lockOpt = locksRepository.findById(new ObjectId(lockId));
        if (!lockOpt.isPresent()) {
            return false;
        }
        return true;
    }
}
