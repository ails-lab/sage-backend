package ac.software.semantic.repository;

import java.util.Optional;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.constants.CampaignState;
import ac.software.semantic.model.constants.CampaignType;
import ac.software.semantic.model.constants.UserRoleType;

@Repository
public interface CampaignRepository extends MongoRepository<Campaign, String> {
    
    Optional<Campaign> findById(ObjectId id);
    
    List<Campaign> findByDatabaseIdAndTypeAndState(ObjectId databaseId, CampaignType type, CampaignState state);

    List<Campaign> findByDatabaseIdAndTypeAndValidatorIdAndState(ObjectId databaseId, CampaignType type, ObjectId validatorId, CampaignState state);

    List<Campaign> findByUserId(ObjectId userId); // should not be used! legacy only
    
    List<Campaign> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
    
    
    List<Campaign> findByDatabaseIdAndUserIdAndType(ObjectId databaseId, ObjectId userId, CampaignType type);

}


