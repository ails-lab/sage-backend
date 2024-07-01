package ac.software.semantic.repository.core;

import java.util.Optional;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.constants.state.CampaignState;
import ac.software.semantic.model.constants.type.CampaignType;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.core.custom.CustomCampaignRepository;

@Repository
public interface CampaignRepository extends DocumentRepository<Campaign>, CustomCampaignRepository {
    
    List<Campaign> findByDatabaseIdAndTypeAndState(ObjectId databaseId, CampaignType type, CampaignState state);
    Page<Campaign> findByDatabaseIdAndTypeAndState(ObjectId databaseId, CampaignType type, CampaignState state, Pageable page);

//    @Query("{ databaseId: ?0, type: ?1, $or: [ { $where: '?2 == null' }, { validatorId: ?2 } ], state: ?3 }")
    List<Campaign> findByDatabaseIdAndTypeAndValidatorIdAndState(ObjectId databaseId, CampaignType type, ObjectId validatorId, CampaignState state);
//    @Query("{ databaseId: ?0, type: ?1, $or: [ { $where: '?2 == null' }, { validatorId: ?2 } ], state: ?3 }")
    Page<Campaign> findByDatabaseIdAndTypeAndValidatorIdAndState(ObjectId databaseId, CampaignType type, ObjectId validatorId, CampaignState state, Pageable page);


    List<Campaign> findByUserId(ObjectId userId); // should not be used! legacy only
    
    List<Campaign> findByDatabaseIdAndUserId(ObjectId databaseId, ObjectId userId);
    
    List<Campaign> findByDatabaseIdAndUserIdAndType(ObjectId databaseId, ObjectId userId, CampaignType type);
    Page<Campaign> findByDatabaseIdAndUserIdAndType(ObjectId databaseId, ObjectId userId, CampaignType type, Pageable page);

}


