package ac.software.semantic.controller;

import ac.software.semantic.model.Access;
import ac.software.semantic.model.constants.AccessType;
import ac.software.semantic.model.constants.DatasetScope;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.constants.DatasetType;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.*;
import ac.software.semantic.service.DatasetService;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import ac.software.semantic.security.CurrentUser;
import ac.software.semantic.security.UserPrincipal;

import ac.software.semantic.service.AccessService;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/access")
@Tag(name = "Access API", description = "Granting and removing access from validators")
public class APIAccessController {

	@Autowired
	private AccessService accessService;

	@Autowired
	private DatasetService datasetService;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
//    @Operation(summary = "Editor gives access to validator for a certain collection. Works with mongo Id", security = @SecurityRequirement(name = "bearerAuth"))
//	@ApiResponses(value = {
//		@ApiResponse(responseCode = "201", description = "Operation Successful",
//		content = { @Content(mediaType = "application/json",
//			schema = @Schema(implementation = AccessItem.class))}),
//		@ApiResponse(responseCode = "400", description = "Access Entry Creation Failed",
//		content = { @Content(mediaType = "application/json",
//			schema = @Schema(implementation = ErrorResponse.class))})
//    })
//	@PostMapping("/createById")
//	public ResponseEntity<?> createAccessEntryById(
//        @Parameter(description = "The userId of validator that will be granted access", example = "1234") @RequestParam String validatorId,
//        @Parameter(description = "The datasetId of collection that validator will receive access rights", example = "1234") @RequestParam String collectionId,
//		@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser
//	) {
//    	try {
//			AccessItem response = accessService.createAccessById(currentUser.getId(), validatorId, collectionId);
//			if (response != null) {
//				return ResponseEntity.ok(response);
//			}
//			else {
//				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
//			}
//		}
//    	catch (Exception ex) {
//			ex.printStackTrace();
//			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
//		}
//
////		 return ResponseEntity.ok(new ErrorResponse("lol"));
//    }

	@Operation(summary = "Editor gives access to validator for a certain collection. Works with virtuoso UUID", security = @SecurityRequirement(name = "bearerAuth"))
	@ApiResponses(value = {
			@ApiResponse(responseCode = "201", description = "Operation Successful",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = AccessItem.class))}),
			@ApiResponse(responseCode = "400", description = "Access Entry Creation Failed",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	@PostMapping("/create")
	public ResponseEntity<?> createAccessEntryByUuid(
			@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			@RequestParam String campaignId,
			@Parameter(description = "The userId of validator that will be granted access", example = "1234") @RequestParam String userId,
			@Parameter(description = "The URI of the dataset that validator will receive access rights", example = "1234") @RequestParam String datasetUri
	) {
		try {
			AccessItem response = accessService.createAccess(new ObjectId(currentUser.getId()), new ObjectId(campaignId), new ObjectId(userId), datasetUri);
			if (response != null) {
				return ResponseEntity.ok(response);
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}
		}
		catch (Exception ex) {
			ex.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}

//		 return ResponseEntity.ok(new ErrorResponse("lol"));
	}

	@Operation(summary = "Editor gives access to validator for all of his owned collections", security = @SecurityRequirement(name = "bearerAuth"))
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "Operation Successful",
					content = { @Content(mediaType = "application/json",
							array = @ArraySchema(schema = @Schema(implementation = AccessItem.class)))}),
			@ApiResponse(responseCode = "400", description = "Access Entry Creation Failed",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	
	@PostMapping("/createAll")
	public ResponseEntity<?> createAllAccessEntries(
			@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			@RequestParam String campaignId,
			@Parameter(description = "The userId of validator that will be granted access", example = "1234") @RequestParam String userId
	) {
		try {
			List<DatasetResponse> ownedDatasets = datasetService.getDatasets(currentUser, DatasetScope.COLLECTION, DatasetType.DATASET);

			List<AccessItem> addedEntries = new ArrayList<>();
			AccessItem res;

			for (DatasetResponse ds : ownedDatasets) {
				if (ds.getPublishState() == DatasetState.PUBLISHED_PRIVATE || ds.getPublishState() == DatasetState.PUBLISHED_PUBLIC || ds.getPublishState() == DatasetState.PUBLISHED) { // should be changed not to allow private datasets

					res = accessService.createAccess(new ObjectId(currentUser.getId()), new ObjectId(campaignId), new ObjectId(userId), resourceVocabulary.getDatasetAsResource(ds.getUuid()).toString());
					if (res != null) {
						addedEntries.add(res);
					}
				}

			}
			if (!addedEntries.isEmpty()) {
				return ResponseEntity.ok(addedEntries);
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}
		}
		catch (Exception ex) {
			ex.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}

	@Operation(summary = "Editor removes every access right assigned to specific validator", security = @SecurityRequirement(name = "bearerAuth"))
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "Operation Successful",
		content = { @Content(mediaType = "application/json",
			schema = @Schema(implementation = AccessRemovalResponse.class))}),
		@ApiResponse(responseCode = "500", description = "Internal Server Error",
		content = { @Content(mediaType = "application/json")})
    })
	@PostMapping("/deleteAll")
	public ResponseEntity<?> removeAllAccessEntries(
			@RequestParam String campaignId,
			@Parameter(description = "The userId of validator for whom access will be revoced", example = "1234") @RequestParam String userId,
			@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser
	) {
		try {
			Long response = accessService.removeAccessEntriesByValidatorId(new ObjectId(currentUser.getId()), new ObjectId(campaignId), new  ObjectId(userId));
			return ResponseEntity.ok(new AccessRemovalResponse(response));
		}
		catch (Exception ex) {
			ex.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}

//    @Operation(summary = "Editor removes access", security = @SecurityRequirement(name = "bearerAuth"))
//	@ApiResponses(value = {
//		@ApiResponse(responseCode = "201", description = "Operation Successful",
//		content = { @Content(mediaType = "application/json",
//			schema = @Schema(implementation = ErrorResponse.class))}),
//		@ApiResponse(responseCode = "400", description = "Access Entry Deletion Failed",
//		content = { @Content(mediaType = "application/json",
//			schema = @Schema(implementation = ErrorResponse.class))})
//    })
//	@DeleteMapping("/{id}")
//	public ResponseEntity<?> deleteAccessEntry(
//		@Parameter(description = "Id of access entry to be removed") @PathVariable("id") String id,
//		@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser
//    ) {
//    	try {
//			//check ownership!
//			boolean ok = accessService.deleteAccessEntry(id);
//			if (ok) {
//				return ResponseEntity.ok(new ErrorResponse("Operation Successful"));
//			}
//			else {
//				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
//			}
//		}
//		catch (Exception ex) {
//			ex.printStackTrace();
//			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
//		}
//    }

	@Operation(summary = "Editor removes access by uuid of dataset and validatorId", security = @SecurityRequirement(name = "bearerAuth"))
	@ApiResponses(value = {
			@ApiResponse(responseCode = "201", description = "Operation Successful",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))}),
			@ApiResponse(responseCode = "400", description = "Access Entry Deletion Failed",
					content = { @Content(mediaType = "application/json",
							schema = @Schema(implementation = ErrorResponse.class))})
	})
	@PostMapping("/delete")
	public ResponseEntity<?> deleteAccess(
			@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser,
			@RequestParam String campaignId,
			@Parameter(description = "UserId of validator") @RequestParam String userId,
			@Parameter(description = "Uri of dataset") @RequestParam String datasetUri
			
	) {
		try {
			
			boolean ok = accessService.deleteAccess(new ObjectId(currentUser.getId()), new ObjectId(campaignId), new ObjectId(userId), datasetUri);
			if (ok) {
				return ResponseEntity.ok(new ErrorResponse("Operation Successful"));
			}
			else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("Operation Failed"));
			}
		}
		catch (Exception ex) {
			ex.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
		}
	}

//	@Operation(summary = "Get access entries.",
//		description = "Dummy implementation. Right now will provide all entries in access table. This will be changed.")
//		//description = "If the call is issued with no parameters, the access entries concerning the logged in user will be returned. If the call is issued with userId, the access entries concerning that specific userId will be returned. If the call is issued with catalogId, the access entries concerning that specific catalogId will be returned. Only editors can issue calls concerning other users / catalogs.")
//	@ApiResponses(value = {
//		@ApiResponse(responseCode = "200", description = "Operation Successful",
//		content = { @Content(mediaType = "application/json",
//			schema = @Schema(implementation = AccessResponse.class))}),
//		@ApiResponse(responseCode = "400", description = "Operation Failed",
//		content = { @Content(mediaType = "application/json",
//			schema = @Schema(implementation = ErrorResponse.class))})
//    })
//	@GetMapping("/getAll")
//	public ResponseEntity<?> getAccessEntry(
//		//@Parameter(description = "The userId for whom we want to get access entries", example = "1234", required=false) @RequestParam(name = "userId", required = false) String userId,
//		//@Parameter(description = "The id of dataset for whom the access entries will be retrieved", example = "1234", required = false) @RequestParam(name = "catalogId", required = false) String catalogId,
//		@Parameter(hidden = true) @CurrentUser UserPrincipal currentUser
//    ) {
//    	return ResponseEntity.ok(new AccessResponse(accessService.getAccessEntries()));
//    }

}