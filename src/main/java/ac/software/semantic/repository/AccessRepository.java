package ac.software.semantic.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

import ac.software.semantic.model.Access;
import ac.software.semantic.model.constants.AccessType;

@Repository
public interface AccessRepository extends MongoRepository<Access, String> {
    
    Optional<Access> findById(String id);

    Long deleteById(ObjectId id);

    Optional<Access> findByCreatorIdAndUserIdAndCollectionUuidAndAccessType(ObjectId creatorId, ObjectId userId, String collectionUuid, AccessType accessType);
    
    List<Access> findByUserIdAndCollectionUuidAndAccessType(ObjectId userId, String collectionUuid, AccessType accessType);

    List<Access> findAll();
    
    List<Access> findByUserIdAndAccessType(ObjectId Id, AccessType accessType);

    List<Access> findByCreatorIdAndUserIdAndAccessType(ObjectId creatorId, ObjectId userId, AccessType accessType);
    
    List<Access> findByCampaignIdAndUserIdAndAccessType(ObjectId campaignId, ObjectId userId, AccessType accessType);
    
    List<Access> findByCampaignIdAndAccessType(ObjectId campaignId, AccessType accessType);

    Optional<Access> findByCampaignIdAndUserIdAndCollectionUuid(ObjectId campaignId, ObjectId userId, String collectionUuid);
    
    Optional<Access> findByCreatorIdAndUserIdAndCollectionUuid(ObjectId creatorId, ObjectId userId, String collectionUuid);

    Optional<Access> findByCreatorIdAndUserIdAndCollectionId(ObjectId creatorId, ObjectId userId, ObjectId collectionId);

    Long deleteByCreatorIdAndUserId(ObjectId creatorId, ObjectId userId);

    Long deleteByCreatorIdAndUserIdAndAccessType(ObjectId creatorId, ObjectId userId, AccessType accessType);

    Long deleteByCampaignId(ObjectId campaignId);

    Long deleteByCampaignIdAndCreatorIdAndUserIdAndCollectionUuidAndAccessType(ObjectId campaignId, ObjectId creatorId, ObjectId userId, String collectionUuid, AccessType accessType);
    
    Long deleteByCampaignIdAndUserIdAndAccessType(ObjectId campaignId, ObjectId userId, AccessType accessType);
    
    Long deleteByCampaignIdAndCreatorIdAndUserIdAndAccessType(ObjectId campaignId, ObjectId creatorId, ObjectId userId, AccessType accessType);
}