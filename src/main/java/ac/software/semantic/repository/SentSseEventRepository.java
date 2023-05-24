package ac.software.semantic.repository;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import ac.software.semantic.model.SentSseEvent;

@Repository
public interface SentSseEventRepository extends JpaRepository <SentSseEvent, Long> {
 
	public List<SentSseEvent> findByIdGreaterThan(long id);
	void deleteByTimestampLessThan(long timestamp);
}
