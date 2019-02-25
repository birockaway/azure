package crossmaster.keboola.wr.azure;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 * @author David Esner
 */
public class AzureBlobService {

	private CloudBlobClient blobClient;

	public AzureBlobService(String azureConnectionString) throws InvalidKeyException, URISyntaxException {
		CloudStorageAccount storageAccount = CloudStorageAccount.parse(azureConnectionString);
		this.blobClient = storageAccount.createCloudBlobClient();
	}

	public void uploadFileToBlobStorage(File file, String containerReference, String blockBlobReference)
			throws FileNotFoundException, StorageException, IOException, URISyntaxException {

		CloudBlobContainer container = blobClient.getContainerReference(containerReference);
		CloudBlockBlob blob = container.getBlockBlobReference(blockBlobReference);
		blob.deleteIfExists();
		blob.upload(new FileInputStream(file), file.length());

	}
}
