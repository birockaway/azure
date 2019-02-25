package crossmaster.keboola.wr.azure;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import crossmaster.keboola.wr.azure.config.AzureWrProperties;
import esnerda.keboola.components.KBCException;
import esnerda.keboola.components.configuration.handler.ConfigHandlerBuilder;
import esnerda.keboola.components.configuration.handler.KBCConfigurationEnvHandler;
import esnerda.keboola.components.logging.DefaultLogger;
import esnerda.keboola.components.logging.KBCLogger;

/**
 * simple writer
 *
 */
public class Writer {

	private static KBCLogger log;

	private static KBCConfigurationEnvHandler handler;
	private static AzureWrProperties config;

	public static void main(String[] args) {
		// init
		log = new DefaultLogger(Writer.class);
		log.info("Configuring environment...");
		try {
			handler = initHandler(args, log);
			config = (AzureWrProperties) handler.getParameters();
			AzureBlobService blobService = new AzureBlobService(config.getAzureConnectionString());
			// get input tables from storage
			List<File> csvInputTables = getInputTables();
			// upload all tables to storage
			log.info("Uploading files to blob storage...");
			for (File csvFile : csvInputTables) {
				log.info("Edit...");
				blobService.uploadFileToBlobStorage(csvFile, config.getDestinationFolder(), csvFile.getName());
			}
			log.info("Upload finished successfully...");
		} catch (KBCException ex) {
			handleException(ex);
		} catch (Exception e) {
			handleException(new KBCException("Import failed! " + e.getMessage(), 2, e));
		}

	}

	private static List<File> getInputTables() throws KBCException {
		if (handler.getInputTables() == null || handler.getInputTables().isEmpty()) {
			throw new KBCException("No input tables specified!", 2, null);
		}
		return handler.getInputTables().stream().map(t -> t.getCsvTable()).collect(Collectors.toList());
	}

	protected static KBCConfigurationEnvHandler initHandler(String[] args, KBCLogger log) throws KBCException {
		KBCConfigurationEnvHandler handler = ConfigHandlerBuilder.create(AzureWrProperties.class).build();
		// process the configuration
		handler.processConfigFile(args);
		return handler;
	}

	protected static void handleException(KBCException ex) {
		log.log(ex);
		if (ex.getSeverity() > 0) {
			System.exit(ex.getSeverity() - 1);
		}
	}
}
