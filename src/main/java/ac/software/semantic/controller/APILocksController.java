package ac.software.semantic.controller;

import ac.software.semantic.model.User;
import ac.software.semantic.payload.ErrorResponse;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.PagedAnnotationValidationPageLocksService;
import io.swagger.v3.oas.annotations.Parameter;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

//DUMMY CONTROLLER FOR LOCKS TESTING!
@RestController
@RequestMapping("/api/locks")
public class APILocksController {
    @Autowired
    private PagedAnnotationValidationPageLocksService locksService;

    @GetMapping(value = "/create/{pavId}")
    public ResponseEntity<?> lock(@CurrentUser UserPrincipal currentUser, @PathVariable String pavId, @RequestParam int page, @RequestParam APIAnnotationEditGroupController.AnnotationValidationRequest mode) {
        try {
            ObjectId result = locksService.obtainLock(currentUser.getId(),pavId, page, mode);
            if (result != null) {
                return ResponseEntity.ok(result.toString());
            }
            else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.ok("Other Exception...");
        }
    }

    @PostMapping(value = "/remove")
    public ResponseEntity<?> removeLock(@CurrentUser UserPrincipal currentUser, @RequestParam String pavId, @RequestParam APIAnnotationEditGroupController.AnnotationValidationRequest mode, @RequestParam int page) {
        try {
            if(locksService.removeLock(currentUser.getId(), pavId, page, mode)) {
                return ResponseEntity.status(HttpStatus.OK).body(new ErrorResponse("Lock successfully removed"));
            }
            else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Lock did not exist"));
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ErrorResponse("Internal server error"));
        }
    }
}
