package ac.software.semantic.controller;

import ac.software.semantic.model.SavedTemplate;
import ac.software.semantic.model.constants.type.TemplateType;
import ac.software.semantic.payload.TemplateDropdownItem;
import ac.software.semantic.payload.TemplateItem;
import ac.software.semantic.payload.response.ErrorResponse;
import ac.software.semantic.payload.response.TemplateResponse;
import ac.software.semantic.payload.response.TemplatesResponse;
import ac.software.semantic.repository.core.SavedTemplateRepository;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ModelMapper;
import ac.software.semantic.service.SavedTemplateService;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/template")
@Tag(name = "Template API", description = "Using predefined templates to load SAGE Configurations.")
public class APITemplateController {

    @Autowired
    private SavedTemplateService templateService;

    @Autowired
    private SavedTemplateRepository templateRepository;
    
    @Autowired
    private ModelMapper modelMapper;

    @GetMapping("/get/{id}")
    public ResponseEntity<?> getById(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @PathVariable String id) {
        Optional<SavedTemplate> tempOpt = templateRepository.findById(new ObjectId(id));
        if (!tempOpt.isPresent()) {
            return null;
        }

        SavedTemplate tmp = tempOpt.get();
        return ResponseEntity.ok(new TemplateItem(tmp));
    }
    
//    @GetMapping(value = "/get-import-templates",
//		        produces = "application/json")
//    public ResponseEntity<?> getImportTemplates(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser) {
//        List<TemplateResponse> catalog = templateService.getCatalogImportTemplates().stream()
//                .map(template -> modelMapper.template2TemplateResponse(template))
//                .collect(Collectors.toList());
//
//    	List<TemplateResponse> dataset = templateService.getDatasetImportTemplates().stream()
//                .map(template -> modelMapper.template2TemplateResponse(template))
//                .collect(Collectors.toList());
//
//    	List<TemplateResponse> mappingSample = templateService.getMappingSampleTemplates().stream()
//                .map(template -> modelMapper.template2TemplateResponse(template))
//                .collect(Collectors.toList());
//
//        return ResponseEntity.ok(new TemplatesResponse(catalog, dataset, mappingSample));
//    }
    
    
    @GetMapping("/get")
    public ResponseEntity<?> getAllTemplatesOfUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam TemplateType type) {
        try {
            List<TemplateDropdownItem> response = templateService.getAllUserTemplates(new ObjectId(currentUser.getId()), type);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }


    @PostMapping("/create")
    public ResponseEntity<?> createTemplate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
                                            @RequestParam String name,
                                            @RequestParam TemplateType type,
                                            @RequestParam(required = false) String templateString,
                                            @RequestBody(required = false) Map<String, Object> requestBody) {
        {
            try {
                TemplateItem response;
                if (type == TemplateType.API_KEY) {
                    response = templateService.createTemplate(new ObjectId(currentUser.getId()), name, type, templateString);
                } else {
                    response = templateService.createTemplate(new ObjectId(currentUser.getId()), name, type, requestBody);
                }

                if (response != null) {
                    return ResponseEntity.ok(response);
                } else {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
            }
        }
    }

//    @PostMapping("/predefined")
//    public ResponseEntity<?> createPredefinedImportTemplate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
//                                            @RequestParam String name,
//                                            @RequestParam TemplateType type,
//                                            @RequestBody String body) {
//        {
//            try {
//                System.out.println(body);
//                templateService.createImportTemplate(new ObjectId(currentUser.getId()), name, type, body);
//                return ResponseEntity.ok(null);
//            } catch (Exception ex) {
//                ex.printStackTrace();
//                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
//            }
//        }
//    }


    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> deleteTemplate(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
                                            @PathVariable String id)
    {
        try {
            Long success = templateRepository.deleteById(new ObjectId(id));
            if (success > 0) {
                return ResponseEntity.ok(null);
            }
            else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }
}
