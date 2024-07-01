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
import ac.software.semantic.model.MappingDocument;
import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.service.lookup.MappingLookupProperties;

@Repository
public class CustomMappingRepositoryImpl implements CustomMappingRepository {
	
	@Autowired
	@Qualifier("coreMongoTemplate")
    private MongoTemplate mongoTemplate;
	
	@Autowired
    @Qualifier("database")
    private Database database;
    
	public boolean existsSameIdentifier(MappingDocument object, IdentifierType type) {
		
		Query query = new Query();

		Criteria cr = Criteria.where("datasetId").is(object.getDatasetId());
		
		if (object.getInstances().size() == 1 && object.getInstances().get(0).getBinding() != null && object.getInstances().get(0).getBinding().size() == 1 &&
				object.getInstances().get(0).getBinding().get(0).getName() == null && object.getInstances().get(0).getBinding().get(0).getValue() == null) { 
				// checking instance identifier > hack: one empty binding to avoid confusion with real mappings
			cr.and("_id").is(object.getId());
			
			query.addCriteria(cr);
			
			List<MappingDocument> res = this.mongoTemplate.find(query, MappingDocument.class);
			if (res.size() == 1) {
				MappingInstance omi = object.getInstances().get(0);
				String identifier = omi.getIdentifier();
				
				MappingDocument mdoc = res.get(0);
				if (mdoc.getInstances() != null) {
					for (MappingInstance mi : mdoc.getInstances()) {
						if (mi.getIdentifier() != null && mi.getIdentifier().equals(identifier)) {
							return true;
						}
					}
				}
				return false;
				
			} else { // shouldn't happen !!!
				return true ;
			}
		
		} else { // checking mapping identifier
			if (type == IdentifierType.IDENTIFIER) {
				cr.and("identifier").is(object.getIdentifier());
			} else {
				return true;
			}
			
			if (object.getId() != null) {
				cr.and("_id").ne(object.getId());
			}
			
			query.addCriteria(cr);
			
			List<MappingDocument> res = this.mongoTemplate.find(query, MappingDocument.class);
			if (res.size() > 0) {
				return true;
			} else {
				return false;
			}

		}
		
	}

	@Override
	public List<MappingDocument> find(ObjectId userId, List<Dataset> dataset, MappingLookupProperties lp, ObjectId databaseId) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId));
		query.with(new Sort(Sort.Direction.ASC, "group", "order"));
		
		return this.mongoTemplate.find(query, MappingDocument.class);
	}

	@Override
	public Page<MappingDocument> find(ObjectId userId, List<Dataset> dataset, MappingLookupProperties lp, ObjectId databaseId, Pageable page) {
		Query query = new Query();
		query.addCriteria(buildCriteria(userId, dataset, lp, databaseId)).with(page);
		query.with(new Sort(Sort.Direction.ASC, "group", "order"));
		
		List<MappingDocument> list = mongoTemplate.find(query, MappingDocument.class);
		
        return PageableExecutionUtils.getPage(list, page, () -> mongoTemplate.count(query, MappingDocument.class));

	}

	private Criteria buildCriteria(ObjectId userId, List<Dataset> dataset, MappingLookupProperties lp, ObjectId databaseId) {
		
		Criteria cr = Criteria.where("databaseId").is(databaseId);
		
		if (userId != null) {
			cr.and("userId").is(userId);
		}
		
		if (dataset != null) {
			cr.and("datasetId").in(dataset.stream().map(p -> p.getId()).collect(Collectors.toList()).toArray());
		}
		
		if (lp != null) {
			if (lp.getMappingType() != null) {
				cr.and("type").is(lp.getMappingType());
			}
			
			if (lp.getGroup() != null) {
				cr.and("group").is(lp.getGroup());
			}
		}
		
		return cr;
	}

}
