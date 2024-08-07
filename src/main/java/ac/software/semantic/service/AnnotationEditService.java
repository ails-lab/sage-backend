package ac.software.semantic.service;

import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.AnnotationEdit;
import ac.software.semantic.model.AnnotationEditGroup;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.PagedAnnotationValidationPage;
import ac.software.semantic.model.constants.type.AnnotationEditType;
import ac.software.semantic.payload.response.AnnotationEditResponse;
import ac.software.semantic.repository.core.AnnotationEditRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;

@Service
public class AnnotationEditService {

	private Logger logger = LoggerFactory.getLogger(AnnotationEditService.class);

    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private AnnotationEditRepository annotationEditRepository;

	
//	public boolean addDeleteEdit(UserPrincipal currentUser, String datasetUuid, List<String> onProperty, String propertyValue, String asProperty, String annotationValue) {
//		
//		List<AnnotationEdit> edits = annotationEditRepository.findByDatasetUuidAndOnPropertyAndPropertyValueAndAsPropertyAndAnnotationValueAndUserId(datasetUuid, onProperty, propertyValue, asProperty, annotationValue, new ObjectId(currentUser.getId()));
//		if (edits.size() > 0) {
//			for (AnnotationEdit e : edits) {
//				if (e.getEditType() == AnnotationEditType.DELETE) {
//					annotationEditRepository.delete(e);
//				}
//			}
//		} else {
//			annotationEditRepository.insert(new AnnotationEdit(datasetUuid, onProperty, propertyValue, asProperty, annotationValue, AnnotationEditType.DELETE, new ObjectId(currentUser.getId())));
//		}
//		
//		return true;
//	}
	
	public boolean processEdit(UserPrincipal currentUser, AnnotationEditGroup aeg, AnnotationEditResponse vad) {
		return processEdit(currentUser, aeg, vad, null);
	}
	
	public boolean processEdit(UserPrincipal currentUser, AnnotationEditGroup aeg, AnnotationEditResponse vad, PagedAnnotationValidationPage pavp) {
		
		//essential assumptions: there should be no edit with addedBy = acceptedBy = rejectedBy = empty.
		//                       addedBy should contain at most one user (the user that created the edit).
		//                       an edit with non-empty addedBy can be deleted only if acceptedBy = rejectedBy = empty.

		int added = 0;
		int validated = 0;
		
		int accepted = 0;
		int rejected = 0;
		int neutral = 0;
		
		ObjectId uid = new ObjectId(currentUser.getId());
		
		Optional<AnnotationEdit> aeOpt; 
		AnnotationEdit edit = null;
		
		if (vad.getId() != null) { // known annotation edit id
			aeOpt = annotationEditRepository.findById(new ObjectId(vad.getId()));
		
			if (aeOpt.isPresent()) {
				edit = aeOpt.get();
			}
		}
		
		if (edit == null) {  // check if same annotation edit exists
			RDFTerm aev = vad.getPropertyValue();
			
			if (aev.getIri() != null) {
				aeOpt = annotationEditRepository.findByAnnotationEditGroupIdAndIriValueAndAnnotationValueAndStartAndEnd(aeg.getId(), aev.getIri(), vad.getAnnotationValue(), vad.getStart() != null ? vad.getStart() : -1, vad.getEnd() != null ? vad.getEnd() : -1);
			} else {
				aeOpt = annotationEditRepository.findByAnnotationEditGroupIdAndLiteralValueAndAnnotationValueAndStartAndEnd(aeg.getId(), aev.getLexicalForm(), aev.getLanguage(), aev.getDatatype(), vad.getAnnotationValue(), vad.getStart() != null ? vad.getStart() : -1, vad.getEnd() != null ? vad.getEnd() : -1 );
			}
			
			if (aeOpt.isPresent()) {
				edit = aeOpt.get();
			}
		}
		
		if (edit != null) {
			// update existing edit

			// previous state, before update
			boolean isAdded = !edit.getAddedByUserId().isEmpty();
			boolean isValidated = !isAdded && (!edit.getAcceptedByUserId().isEmpty() || !edit.getRejectedByUserId().isEmpty()); 
			
			boolean isAccepted = isValidated && edit.getAcceptedByUserId().size() > edit.getRejectedByUserId().size();
			boolean isRejected = isValidated && edit.getAcceptedByUserId().size() < edit.getRejectedByUserId().size();
			boolean isNeutral = isValidated && edit.getAcceptedByUserId().size() == edit.getRejectedByUserId().size();
			
//				if (vad.getEditType() == null) { // delete
//					                             // it should be possible to delete the edit only if accepted = rejected = empty
//					                             // and only by the user that create it
//					if (!edit.getAcceptedByUserId().isEmpty() || !edit.getRejectedByUserId().isEmpty() || 
//							edit.getAddedByUserId().size() != 1 || !edit.getAddedByUserId().contains(uid)) {
//						
//						logger.error("Error deleting annotation edit " + edit.getId());
//						return false;
//					}
//				}
					
			if (vad.getEditType() == null) { // delete 
				edit.getAcceptedByUserId().remove(uid);  
				edit.getRejectedByUserId().remove(uid); 
				edit.getAddedByUserId().remove(uid);    // that should be the user that created the edit
				
				edit.removeTargetAcceptPropertyForUserId(uid);
				
			} else if (vad.getEditType() == AnnotationEditType.ADD) { // modify value
				edit.setAnnotationValue(vad.getAnnotationValue());
			
				edit.addTargetAcceptPropertyForUserId(uid, vad.getTarget());
				
				edit.setReferences(vad.getReferences());

			} else if (vad.getEditType() == AnnotationEditType.ACCEPT) {
				edit.getAcceptedByUserId().add(uid); 
				
				edit.addTargetAcceptPropertyForUserId(uid, vad.getTarget());
				
			} else if (vad.getEditType() == AnnotationEditType.REJECT) {
				edit.getRejectedByUserId().add(uid);
			
			} else if (vad.getEditType() == AnnotationEditType.CHANGE_TARGET) { // modify target
				edit.removeTargetAcceptPropertyForUserId(uid);
				edit.addTargetAcceptPropertyForUserId(uid, vad.getTarget());
			}
			
			// remove edit. there should be no edit with added = accepted = rejected = empty.
			if (edit.getAddedByUserId().isEmpty() && edit.getAcceptedByUserId().isEmpty() && edit.getRejectedByUserId().isEmpty()) {
				annotationEditRepository.delete(edit);
				
				if (isAdded) {
					added -= vad.getCount();
				} else if (isValidated) {
					validated -= vad.getCount();
					
					if (isAccepted) {
						accepted -= vad.getCount();
					} else if (isRejected) {
						rejected -= vad.getCount();
					} else if (isNeutral) {
						neutral -= vad.getCount();
					}
				}
				
			// edit not deleted
			} else {
				
				if (isAdded) {
					
				} else if (vad.getEditType() == AnnotationEditType.CHANGE_TARGET) {
					
				} else if (isValidated) { //if it was validated is should still be validated, otherwise it should have been deleted.
					
					if (isAccepted) {
						accepted -= vad.getCount();
					} else if (isRejected) {
						rejected -= vad.getCount();
					} else if (isNeutral) {
						neutral -= vad.getCount();
					}
					
					isAccepted = edit.getAcceptedByUserId().size() > edit.getRejectedByUserId().size();
					isRejected = edit.getAcceptedByUserId().size() < edit.getRejectedByUserId().size();
					isNeutral = edit.getAcceptedByUserId().size() == edit.getRejectedByUserId().size();

					if (isAccepted) {
						accepted += vad.getCount();
					} else if (isRejected) {
						rejected += vad.getCount();
					} else if (isNeutral) {
						neutral += vad.getCount();
					}
				}
				
				annotationEditRepository.save(edit);
			}
		
		} else  { // create new edit

			AnnotationEdit ae = new AnnotationEdit();
			
			ae.setAnnotationEditGroupId(aeg.getId());
			ae.setDatabaseId(database.getId());
			ae.setDatasetUuid(aeg.getDatasetUuid()); // should be deleted 
			ae.setOnProperty(aeg.getOnProperty());   // should be deleted
			ae.setAsProperty(aeg.getAsProperty());   // should be deleted
			ae.setOnValue(vad.getPropertyValue());
			ae.setAnnotationValue(vad.getAnnotationValue());
			ae.setStart(vad.getStart() != null ? vad.getStart() : -1);
			ae.setEnd(vad.getEnd() != null ? vad.getEnd() : -1);
			
			ae.setTag(aeg.getTag());
			
			if (vad.getEditType() == AnnotationEditType.ADD) {
				ae.getAddedByUserId().add(uid); 
				added += vad.getCount();
				
				ae.addTargetAcceptPropertyForUserId(uid, vad.getTarget());
				
				ae.setReferences(vad.getReferences());
				
			} else if (vad.getEditType() == AnnotationEditType.ACCEPT) {
				ae.getAcceptedByUserId().add(uid);
				validated += vad.getCount();
				accepted += vad.getCount();
				
				ae.addTargetAcceptPropertyForUserId(uid, vad.getTarget());
				
			} else if (vad.getEditType() == AnnotationEditType.REJECT) {
				ae.getRejectedByUserId().add(uid);
				validated += vad.getCount();
				rejected += vad.getCount();
			}

			if (pavp != null) {
				ae.setPagedAnnotationValidationId(pavp.getPagedAnnotationValidationId()); 
			}
			
			annotationEditRepository.save(ae);

		}
		
		if (pavp != null && vad.getEditType() != AnnotationEditType.CHANGE_TARGET) {
		    pavp.setAddedCount(Math.max(0,pavp.getAddedCount() + added));
		    pavp.setValidatedCount(Math.max(0,pavp.getValidatedCount() + validated));
		    pavp.setUnvalidatedCount(Math.max(0,pavp.getAnnotationsCount() - pavp.getValidatedCount()));
		    pavp.setAcceptedCount(Math.max(0,pavp.getAcceptedCount() + accepted));
		    pavp.setRejectedCount(Math.max(0,pavp.getRejectedCount() + rejected));
		    pavp.setNeutralCount(Math.max(0,pavp.getNeutralCount() + neutral));
		}

		return true;
	}

	
	public List<AnnotationEdit> getAll(UserPrincipal currentUser, String datasetUuid, List<String> onProperty, String asProperty) {
		return annotationEditRepository.findByDatasetUuidAndOnPropertyAndAsPropertyAndUserId(datasetUuid, onProperty, asProperty, new ObjectId(currentUser.getId()));
	}
	

}
