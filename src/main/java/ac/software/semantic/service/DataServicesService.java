package ac.software.semantic.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataService.DataServiceType;
import ac.software.semantic.model.base.ServiceDocument;
import ac.software.semantic.repository.root.DataServiceRepository;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.DataServiceRank;
import ac.software.semantic.model.DataServiceVariant;

@Service
public class DataServicesService {

	@Value("${dataservice.definition.folder}")
	private String dataserviceFolder;
	
	@Autowired
	private DataServiceRepository dataServiceRepository;
	
	@Autowired
	private ResourceLoader resourceLoader;

	public String readMappingDocument(ServiceDocument sdoc, Map<String, Object> params, DataServiceRank rank) throws Exception {
		Optional<DataService> dsOpt = dataServiceRepository.findByIdentifierAndType(sdoc.getIdentity(), sdoc.getType());
		if (!dsOpt.isPresent()) {
			throw new Exception("Data service " + sdoc.getIdentity() + " not found");
		}
		
		DataService ds = dsOpt.get(); 

		// put default values if not exist
		if (ds.getParameters() != null) {
			for (DataServiceParameter dsp : ds.getParameters()) {
				if (!params.containsKey(dsp.getName()) && dsp.getDefaultValue() != null) {
					params.put(dsp.getName(), dsp.getDefaultValue());
				}
			}
		}
		
		String variant = sdoc.getVariant();
		List<DataServiceVariant> variants = ds.getVariants();

		String d2rmlPath = null;
		if (variant == null) {
			d2rmlPath = variants.get(0).getD2rml();
		} else {
			for (int i = 0; i < variants.size(); i++) {
				DataServiceVariant var = variants.get(i); 
				if (var.getName().equals(sdoc.getVariant()) && var.getRank() == rank) {
					d2rmlPath = var.getD2rml();
					break;
				}
			}
		}
		
		d2rmlPath = dataserviceFolder + d2rmlPath; 

//		System.out.println("Loading " + d2rmlPath);
		
		String str = null;
		try (InputStream inputStream = resourceLoader.getResource("classpath:" + d2rmlPath).getInputStream()) {
			str = new String(FileCopyUtils.copyToByteArray(inputStream), StandardCharsets.UTF_8);
		}
		
		return str;
		
	}
}
