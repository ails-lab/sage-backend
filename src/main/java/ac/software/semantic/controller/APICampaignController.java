package ac.software.semantic.controller;

import java.util.List;
import java.util.stream.Collectors;

import ac.software.semantic.payload.*;
import ac.software.semantic.repository.CampaignRepository;
import ac.software.semantic.service.CampaignService;
import ac.software.semantic.service.ModelMapper;
import io.swagger.v3.oas.annotations.Parameter;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.constants.CampaignState;
import ac.software.semantic.model.constants.CampaignType;
import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;

@RestController
@RequestMapping("/api/campaign")
public class APICampaignController {

    @Autowired
    @Qualifier("database")
    private Database database;

	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	private CampaignRepository campaignRepository;

	@Autowired
	private CampaignService campaignService;


    @PostMapping(value = "/create")
	public ResponseEntity<?> createCampaign(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestBody CampaignRequest body) {
			                               
		Campaign camp = campaignService.create(new ObjectId(currentUser.getId()), body.getName(), CampaignType.ANNOTATION_VALIDATION, body.getState());
		
		return ResponseEntity.ok(modelMapper.campaign2CampaignResponse(camp));
	}
    
    @PostMapping(value = "/update/{id}")
	public ResponseEntity<?> updateCampaign(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable("id") String id, @RequestBody CampaignRequest body) {
			                               
		Campaign camp = campaignService.update(new ObjectId(id), body.getName(), body.getState());
		
		return ResponseEntity.ok(modelMapper.campaign2CampaignResponse(camp));
	}
    
    @DeleteMapping(value = "/delete/{id}")
	public ResponseEntity<?> deleteCampaign(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,  @PathVariable("id") String id) {
			                               
		boolean res = campaignService.delete(new ObjectId(id));
		
		if (res) {
			return new ResponseEntity<>(APIResponse.SuccessResponse("The campaign has been deleted."), HttpStatus.OK);
		} else {
			return new ResponseEntity<>(APIResponse.FailureResponse("The campaign could not be deleted."), HttpStatus.NOT_FOUND);	
		}
		
		
	}

	@GetMapping("/get-active-campaigns")
	public ResponseEntity<?> getActiveCampaigns(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam CampaignType type) {
		List<Campaign> docs = campaignRepository.findByDatabaseIdAndTypeAndState(database.getId(), type, CampaignState.ACTIVE);
		
		return ResponseEntity.ok(docs.stream().map(doc -> modelMapper.campaign2CampaignResponse(doc)).collect(Collectors.toList()));
	}
	
	@GetMapping("/get-joined-campaigns")
	public ResponseEntity<?> getJoinedCampaigns(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("type") CampaignType type) {
		List<Campaign> docs = campaignRepository.findByDatabaseIdAndTypeAndValidatorIdAndState(database.getId(), type, new ObjectId(currentUser.getId()), CampaignState.ACTIVE);
		
		return ResponseEntity.ok(docs.stream().map(doc -> modelMapper.campaign2CampaignResponse(doc)).collect(Collectors.toList()));
	}
	
	@GetMapping("/get-owned-campaigns")
	public ResponseEntity<?> getOwnedCampaigns(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("type") CampaignType type) {
		List<CampaignResponse> docs = campaignService.getOwnedCampaigns(new ObjectId(currentUser.getId()), type);
		
		return ResponseEntity.ok(docs);
	}
	
	@PostMapping("/join")
	public ResponseEntity<?> joinCampaign(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("campaignId") String campaignId) {
		try {
			boolean success = campaignService.addUserToCampaign(new ObjectId(currentUser.getId()), new ObjectId(campaignId), new ObjectId(currentUser.getId()));
			if (success) {
				return ResponseEntity.ok(new ErrorResponse("Validator successfully added to campaign"));
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}
	
	@PostMapping("/remove-user")
	public ResponseEntity<?> removeUser(@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser, @RequestParam("campaignId") String campaignId, @RequestParam("userId") String userId) {
		try {
			boolean success = campaignService.removeUserFromCampaign(new ObjectId(currentUser.getId()), new ObjectId(campaignId), new ObjectId(userId));
			if (success) {
				return ResponseEntity.ok(new ErrorResponse("Validator successfully added to campaign"));
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}


}
