package ac.software.semantic.repository.core;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

import ac.software.semantic.model.Access;
import ac.software.semantic.model.constants.type.UserRoleType;

@Repository
public interface AccessRepository extends MongoRepository<Access, String> {
    
    Optional<Access> findById(String id);

    Long deleteById(ObjectId id);

    Optional<Access> findByCreatorIdAndUserIdAndCollectionUuidAndAccessType(ObjectId creatorId, ObjectId userId, String collectionUuid, UserRoleType accessType);
    
    List<Access> findByUserIdAndCollectionUuidAndAccessType(ObjectId userId, String collectionUuid, UserRoleType accessType);
    
    List<Access> findByUserIdAndCollectionIdAndAccessType(ObjectId userId, ObjectId collectionId, UserRoleType accessType);
    
    default List<Access> findByUserIdAndDatasetIdAndAccessType(ObjectId userId, ObjectId datasetId, UserRoleType accessType) {
    	return findByUserIdAndCollectionIdAndAccessType(userId, datasetId, accessType);
    }
    
    List<Access> findAll();
    
    List<Access> findByUserIdAndAccessType(ObjectId Id, UserRoleType accessType);

    List<Access> findByCreatorIdAndUserIdAndAccessType(ObjectId creatorId, ObjectId userId, UserRoleType accessType);
    
    List<Access> findByCampaignIdAndUserIdAndAccessType(ObjectId campaignId, ObjectId userId, UserRoleType accessType);
    
    List<Access> findByCampaignIdAndAccessType(ObjectId campaignId, UserRoleType accessType);

    Optional<Access> findByCampaignIdAndUserIdAndCollectionId(ObjectId campaignId, ObjectId userId, ObjectId collectionId);
    
    Optional<Access> findByCreatorIdAndUserIdAndCollectionUuid(ObjectId creatorId, ObjectId userId, String collectionUuid);

    Optional<Access> findByCreatorIdAndUserIdAndCollectionId(ObjectId creatorId, ObjectId userId, ObjectId collectionId);

    Long deleteByCreatorIdAndUserId(ObjectId creatorId, ObjectId userId);

    Long deleteByCreatorIdAndUserIdAndAccessType(ObjectId creatorId, ObjectId userId, UserRoleType accessType);

    Long deleteByCampaignId(ObjectId campaignId);

    Long deleteByCampaignIdAndCreatorIdAndUserIdAndCollectionUuidAndAccessType(ObjectId campaignId, ObjectId creatorId, ObjectId userId, String collectionUuid, UserRoleType accessType);
    Long deleteByCampaignIdAndCreatorIdAndUserIdAndCollectionIdAndAccessType(ObjectId campaignId, ObjectId creatorId, ObjectId userId, ObjectId collectionId, UserRoleType accessType);
    
    Long deleteByCampaignIdAndUserIdAndAccessType(ObjectId campaignId, ObjectId userId, UserRoleType accessType);
    
    Long deleteByCampaignIdAndCreatorIdAndUserIdAndAccessType(ObjectId campaignId, ObjectId creatorId, ObjectId userId, UserRoleType accessType);
}