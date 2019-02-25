package crossmaster.keboola.wr.azure.config;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import esnerda.keboola.components.configuration.IKBCParameters;
import esnerda.keboola.components.configuration.ValidationException;

/**
 * @author David Esner
 */
public class AzureWrProperties extends IKBCParameters {
	private final static String[] REQUIRED_FIELDS = { "azureConnectionString" };
	private final Map<String, Object> parametersMap;

	/* auth */
	@JsonProperty("#azureConnectionString")
	private String azureConnectionString;

	@JsonProperty("destinationFolder")
	private String destinationFolder;
	
	@JsonProperty("account")
	private String account;

	@JsonCreator
	public AzureWrProperties(@JsonProperty("#azureConnectionString") String azureConnectionString,
			@JsonProperty("destinationFolder") String destinationFolder, @JsonProperty("account") String account) {

		this.azureConnectionString = azureConnectionString;
		this.destinationFolder = destinationFolder;
		this.account = account;
		// set param map
		parametersMap = new HashMap<>();
		parametersMap.put("azureConnectionString", azureConnectionString);
	}

	@Override
	protected String[] getRequiredFields() {
		return REQUIRED_FIELDS;
	}

	@Override
	protected boolean validateParametres() throws ValidationException {
		// validate date format
		String error = "";

		error += this.missingFieldsMessage(parametersMap);
		if (error.equals("")) {
			return true;
		}
		throw new ValidationException("Invalid configuration parameters!", "Config validation error: " + error, null);
	}

	public String getAzureConnectionString() {
		return azureConnectionString;
	}

	public void setAzureConnectionString(String azureConnectionString) {
		this.azureConnectionString = azureConnectionString;
	}

	public String getDestinationFolder() {
		return destinationFolder;
	}

	public void setDestinationFolder(String destinationFolder) {
		this.destinationFolder = destinationFolder;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	


	

}
