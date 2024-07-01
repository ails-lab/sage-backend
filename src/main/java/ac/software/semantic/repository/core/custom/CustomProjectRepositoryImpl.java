package ac.software.semantic.repository.core.custom;

import java.util.List;

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
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.service.lookup.ProjectLookupProperties;

@Repository
public class CustomProjectRepositoryImpl implements CustomProjectRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;

	@Autowired
    @Qualifier("database")
    private Database database;

	public List<ProjectDocument> find(ObjectId userId, ProjectLookupProperties lp, ObjectId databaseId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, lp, databaseId));
		query.with(new Sort(Sort.Direction.ASC, "name"));
		
		return this.mongoTemplate.find(query, ProjectDocument.class);
	}
	
	public Page<ProjectDocument> find(ObjectId userId, ProjectLookupProperties lp, ObjectId databaseId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, lp, databaseId)).with(page);
		query.with(new Sort(Sort.Direction.ASC, "name"));
		
		List<ProjectDocument> list = mongoTemplate.find(query, ProjectDocument.class);
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, ProjectDocument.class));
	}
	
	private Criteria buildCriteria(ObjectId userId, ProjectLookupProperties lp, ObjectId databaseId) {
		
		Criteria cr = Criteria.where("databaseId").is(databaseId);
		
		if (userId != null) {
			cr.and("userId").is(userId);
		}
		
		if (lp != null) {
			if (lp.getUserIdNot() != null) {
				cr.and("userId").ne(lp.getUserIdNot());
			}
	
			if (lp.getJoinedUserId() != null) {
				cr.and("joinedUserId").is(lp.getJoinedUserId());
			}
	
			if (lp.getPublik() != null) {
				cr.and("publik").is(lp.getPublik());
			}
		}
		
		return cr;
	}

	@Override
	public boolean existsSameIdentifier(ProjectDocument object, IdentifierType type) {
		
		Query query = new Query();
		
		Criteria cr = Criteria.where("databaseId").is(object.getDatabaseId());
		
		if (type == IdentifierType.IDENTIFIER) {
			cr.and("identifier").is(object.getIdentifier());
		} else {
			return false;
		}
		
		if (object.getId() != null) {
			cr.and("_id").ne(object.getId());
		}

		query.addCriteria(cr);
		
		List<ProjectDocument> res = this.mongoTemplate.find(query, ProjectDocument.class);
		if (res.size() > 0) {
			return true;
		} else {
			return false;
		}
	}

}
