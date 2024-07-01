package ac.software.semantic.repository.core.custom;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.Campaign;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.service.lookup.CampaignLookupProperties;

public interface CustomCampaignRepository {

	public List<Campaign> find(ObjectId userId, List<ProjectDocument> project, CampaignLookupProperties lp, ObjectId databaseId);
	public Page<Campaign> find(ObjectId userId, List<ProjectDocument> project, CampaignLookupProperties lp, ObjectId databaseId, Pageable page);
}
