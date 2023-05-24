package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Access;
import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.AccessType;
import ac.software.semantic.model.constants.CampaignState;
import ac.software.semantic.model.constants.CampaignType;
import ac.software.semantic.payload.CampaignResponse;
import ac.software.semantic.payload.NewUserSummary;
import ac.software.semantic.repository.AccessRepository;
import ac.software.semantic.repository.CampaignRepository;
import ac.software.semantic.repository.UserRepository;

@Service
public class CampaignService {

	Logger logger = LoggerFactory.getLogger(CampaignService.class);
	
    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private ModelMapper modelMapper;

	@Autowired
	private AccessRepository accessRepository;
	
	@Autowired
	private UserRepository userRepository;

	@Autowired
	private CampaignRepository campaignRepository;
	
	public Campaign create(ObjectId userId, String name, CampaignType type, CampaignState state) {
		Campaign camp = new Campaign();
		camp.setDatabaseId(database.getId());
		camp.setName(name);
		camp.setUserId(userId);
		camp.setUuid(UUID.randomUUID().toString());
		camp.setType(type);
		camp.setState(state);
		camp.setCreatedAt(new Date());
		
		camp = campaignRepository.save(camp);
		
		return camp;
	}
	
	public Campaign update(ObjectId id, String name, CampaignState state) {
		
		Optional<Campaign> campOpt = campaignRepository.findById(id);
		
		if (!campOpt.isPresent()) {
			return null;
		}
		
		Campaign camp = campOpt.get();

		if (name != null) {
			camp.setName(name);
		}
		
		camp.setState(state);

		camp = campaignRepository.save(camp);
		
		return camp;
	}
	
	public boolean delete(ObjectId id) {
		
		Optional<Campaign> campOpt = campaignRepository.findById(id);
		
		if (!campOpt.isPresent()) {
			return false;
		}
		
		Campaign camp = campOpt.get();

		accessRepository.deleteByCampaignId(camp.getId());

		campaignRepository.delete(camp);
		
		return true;
	}
	
	public boolean addUserToCampaign(ObjectId campaignOwnerId, ObjectId campaignId, ObjectId userId) {

		/// -- legacy >
		Optional<User> editorOpt = userRepository.findById(campaignOwnerId);
		if (editorOpt.isPresent()) {
			User editor = editorOpt.get();
			List<String> validators = editor.getValidatorList().stream()
					.map(validatorId -> validatorId.toString())
					.collect(Collectors.toList());
			if (validators.contains(userId.toString())) {
//				return false;
			}
			else {
				editor.addValidator(userId);
				userRepository.save(editor);
//				return true;
			}
		}
		/// -- legacy <

		Optional<Campaign> campOpt = campaignRepository.findById(campaignId);
		if (!campOpt.isPresent()) {
			return false;
		}
		
		Campaign campaign = campOpt.get();

		if (campaign.addValidatorId(userId)) {
			campaignRepository.save(campaign);
			return true;
		} else {
			return false;
		}
	}
	
	public boolean removeUserFromCampaign(ObjectId campaignOwnerId, ObjectId campaignId, ObjectId userId) {

		/// -- legacy >
		Optional<User> editorOpt = userRepository.findById(campaignOwnerId);
		if (editorOpt.isPresent()) {
			User editor = editorOpt.get();
			
			Set<String> allValidators = new HashSet<>();
			
			for (Campaign camp : campaignRepository.findByUserId(campaignOwnerId)) {
				if (!camp.getId().toString().equals(campaignId.toString())) {
					for (ObjectId id : camp.getValidatorId()) {
						allValidators.add(id.toString());
					}
				}
			}
			
			if (!allValidators.contains(userId.toString())) {
				List<ObjectId> validators = editor.getValidatorList();
				boolean success = validators.remove(userId);
				editor.setValidatorList(validators);
			
				List<Access> accessEntries = accessRepository.findByCreatorIdAndUserIdAndAccessType(campaignOwnerId, userId, AccessType.VALIDATOR);
				for (Access acc : accessEntries) {
					accessRepository.delete(acc);
				}
				userRepository.save(editor);
			}
//			return true;
		}
		else {
//			return false;
		}
		/// -- legacy <
		
		Optional<Campaign> campOpt = campaignRepository.findById(campaignId);
		if (!campOpt.isPresent()) {
			return false;
		}
		
		Campaign campaign = campOpt.get();
		
		if (campaign.removeValidatorId(userId)) {
			
			accessRepository.deleteByCampaignIdAndUserIdAndAccessType(campaign.getId(), userId, AccessType.VALIDATOR);
			campaignRepository.save(campaign);
			
			return true;
		} else {
			return false;
		}
		
	}	
	
	public List<CampaignResponse> getOwnedCampaigns(ObjectId userId, CampaignType type)  {
		List<Campaign> campaigns = campaignRepository.findByDatabaseIdAndUserIdAndType(database.getId(), userId, type);
		
		List<CampaignResponse> res = new ArrayList<>();
		
		for (Campaign camp : campaigns) {
			List<User> validators = new ArrayList<User>();
			if (camp.getValidatorId() != null) {
				for (ObjectId validatorId : camp.getValidatorId()) {
					Optional<User> retrievedUser = userRepository.findById(validatorId.toString());
					if (retrievedUser.isPresent()) {
						validators.add(retrievedUser.get());
					}
				}
			}
			
			List<NewUserSummary> users = validators.stream()
				.map(validator -> new NewUserSummary(validator))
				.collect(Collectors.toList());
			
			CampaignResponse cr = modelMapper.campaign2CampaignResponse(camp);
			if (users.size() > 0) {
				cr.setUsers(users);
			}
			
			res.add(cr);
		}
			
		return res;
		
	}

	
}
