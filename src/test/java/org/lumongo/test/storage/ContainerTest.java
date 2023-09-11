package org.lumongo.test.storage;

import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public abstract class ContainerTest {
    	protected MongoDBContainer mongdbContainer;
	protected MongoClient mongoClient;

	@BeforeClass
	public void up() {
		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"
		));
		mongdbContainer.start();

		mongoClient = MongoClients.create(mongdbContainer.getConnectionString());
	}

	@AfterClass
	public void down() {
		mongoClient.close();
		mongdbContainer.stop();
	}
}
