package ac.software.semantic.repository.core.custom;

import java.util.List;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.service.lookup.CampaignLookupProperties;

@Repository
public class CustomCampaignRepositoryImpl implements CustomCampaignRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
    
	public List<Campaign> find(ObjectId userId, List<ProjectDocument> project, CampaignLookupProperties lp, ObjectId databaseId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, project, lp, databaseId));
		query.with(new Sort(Sort.Direction.ASC, "name"));
		
		return this.mongoTemplate.find(query, Campaign.class);
	}
	
	public Page<Campaign> find(ObjectId userId, List<ProjectDocument> project, CampaignLookupProperties lp, ObjectId databaseId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, project, lp, databaseId)).with(page);
		query.with(new Sort(Sort.Direction.ASC, "name"));
		
		List<Campaign> list = mongoTemplate.find(query, Campaign.class);
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, Campaign.class));
	}
	
	private Criteria buildCriteria(ObjectId userId, List<ProjectDocument> project, CampaignLookupProperties lp, ObjectId databaseId) {
		
		Criteria cr = Criteria.where("databaseId").is(databaseId);
		
		if (userId != null) {
			cr.and("userId").is(userId);
		}

		if (project != null) {
			cr.and("projectId").in(project.stream().map(p -> p.getId()).collect(Collectors.toList()).toArray());
		}

		if (lp != null) {
		
			if (lp.getCampaignType() != null) {
				cr.and("type").is(lp.getCampaignType());
			}
			
			if (lp.getCampaignState() != null) {
				cr.and("state").in(lp.getCampaignState());
			}
			
			if (lp.getValidatorId() != null) {
				cr.and("validatorId").is(lp.getValidatorId());
			}
		}
		
		return cr;
	}

}
