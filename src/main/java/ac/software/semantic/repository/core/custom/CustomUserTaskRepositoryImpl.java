package ac.software.semantic.repository.core.custom;

import java.util.List;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DistributionDocument;
import ac.software.semantic.model.IndexDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.UserTaskDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.service.lookup.UserTaskLookupProperties;

@Repository
public class CustomUserTaskRepositoryImpl implements CustomUserTaskRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
    
	@Autowired
    @Qualifier("database")
    private Database database;
    
	@Override
	public List<UserTaskDocument> find(ObjectId userId, List<Dataset> dataset, UserTaskLookupProperties lp, ObjectId databaseId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId));
//		query.with(new Sort(Sort.Direction.ASC, "group"));
//		query.with(new Sort(Sort.Direction.ASC, "order"));
		
		return this.mongoTemplate.find(query, UserTaskDocument.class);
	}

	@Override
	public Page<UserTaskDocument> find(ObjectId userId, List<Dataset> dataset, UserTaskLookupProperties lp, ObjectId databaseId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId)).with(page);
//		query.with(new Sort(Sort.Direction.ASC, "group"));
//		query.with(new Sort(Sort.Direction.ASC, "order"));
		
		List<UserTaskDocument> list = mongoTemplate.find(query, UserTaskDocument.class);
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, UserTaskDocument.class));

	}

	private Criteria buildCriteria(ObjectId userId, List<Dataset> dataset, UserTaskLookupProperties lp, ObjectId databaseId) {
		
		Criteria cr = Criteria.where("databaseId").is(databaseId);
		
		if (userId != null) {
			cr.and("userId").is(userId);
		}
		
		if (dataset != null) {
			cr.and("datasetId").in(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()).toArray());
		}
		
		if (lp != null) {
			if (lp.getFileSystemConfigurationId() != null) {
				cr.and("fileSystemConfigurationId").in(lp.getFileSystemConfigurationId());
			}
		}
		
		return cr;
	}

}
