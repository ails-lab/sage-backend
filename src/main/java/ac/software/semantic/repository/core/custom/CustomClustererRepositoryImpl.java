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

import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.ClustererDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.service.lookup.AnnotatorLookupProperties;
import ac.software.semantic.service.lookup.ClustererLookupProperties;

@Repository
public class CustomClustererRepositoryImpl implements CustomClustererRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
	
	@Autowired
    @Qualifier("database")
    private Database database;
    
	public boolean existsSameIdentifier(ClustererDocument object, IdentifierType type) {
		
		Query query = new Query();

		Criteria cr = Criteria.where("datasetId").is(object.getDatasetId());
		
		if (type == IdentifierType.IDENTIFIER) {
			cr.and("identifier").is(object.getIdentifier());
		} else {
			return true;
		}
		
		if (object.getId() != null) {
			cr.and("_id").ne(object.getId());
		}
		
		query.addCriteria(cr);
		
		List<AnnotatorDocument> res = this.mongoTemplate.find(query, AnnotatorDocument.class);
		if (res.size() > 0) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public List<ClustererDocument> find(ObjectId userId, List<Dataset> dataset, ClustererLookupProperties lp, ObjectId databaseId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId));
//		query.with(new Sort(Sort.Direction.ASC, "group"));
//		query.with(new Sort(Sort.Direction.ASC, "order"));
		
		return this.mongoTemplate.find(query, ClustererDocument.class);
	}

	@Override
	public Page<ClustererDocument> find(ObjectId userId, List<Dataset> dataset, ClustererLookupProperties lp, ObjectId databaseId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId)).with(page);
//		query.with(new Sort(Sort.Direction.ASC, "group"));
//		query.with(new Sort(Sort.Direction.ASC, "order"));
		
		List<ClustererDocument> list = mongoTemplate.find(query, ClustererDocument.class);
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, ClustererDocument.class));

	}

	private Criteria buildCriteria(ObjectId userId, List<Dataset> dataset, ClustererLookupProperties lp, ObjectId databaseId) {
		
		Criteria cr = Criteria.where("databaseId").is(databaseId);
		
		if (userId != null) {
			cr.and("userId").is(userId);
		}
		
		if (dataset != null) {
			cr.and("datasetId").in(dataset.stream().map(e -> e.getId()).collect(Collectors.toList()).toArray());
		}
		
		return cr;
	}

}
