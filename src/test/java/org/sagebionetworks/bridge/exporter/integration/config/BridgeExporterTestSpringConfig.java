package org.sagebionetworks.bridge.exporter.integration.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.dynamodb.DynamoScanHelper;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;

/**
 * Test-only Spring config that mocks out things we don't want in our test configs for reliability/repeatability
 * concerns, most notably Redis.
 */
@Configuration
public class BridgeExporterTestSpringConfig {
    private static final String CONFIG_FILE = "BridgeExporter-test.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    @Bean
    public Config bridgeConfig() {
        String defaultConfig = getClass().getClassLoader().getResource(DEFAULT_CONFIG_FILE).getPath();
        Path defaultConfigPath = Paths.get(defaultConfig);
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        try {
            if (Files.exists(localConfigPath)) {
                return new PropertiesConfig(defaultConfigPath, localConfigPath);
            } else {
                return new PropertiesConfig(defaultConfigPath);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Bean(name = "awsCredentials")
    public BasicAWSCredentials awsCredentials() {
        return new BasicAWSCredentials(bridgeConfig().get("aws.key"),
                bridgeConfig().get("aws.secret.key"));
    }

    @Bean
    public SqsHelper sqsHelper() {
        SqsHelper sqsHelper = new SqsHelper();
        sqsHelper.setSqsClient(new AmazonSQSClient(awsCredentials()));
        return sqsHelper;
    }

    @Bean
    public S3Helper s3Helper() {
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(new AmazonS3Client(awsCredentials()));
        return s3Helper;
    }

    @Bean
    public AmazonS3Client s3Client() {
        return new AmazonS3Client(awsCredentials());
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient(awsCredentials()));
    }

    @Bean(name = "ddbPrefix")
    public String ddbPrefix() {
        Config config = bridgeConfig();
        String envName = config.getEnvironment().name().toLowerCase();
        String userName = config.getUser();
        return envName + '-' + userName + '-';
    }

    @Bean(name = "env")
    public String env() {
        Config config = bridgeConfig();
        String envName = config.getEnvironment().name().toLowerCase();
        return envName;
    }

    @Bean(name = "ddbExportTimeTable")
    public Table ddbExportTimeTable() {

        return ddbClient().getTable(ddbPrefix() + "ExportTime");
    }

    @Bean(name = "ddbSynapseMetaTables")
    public Table ddbSynapseMetaTables() {
        return ddbClient().getTable(ddbPrefix() + "SynapseMetaTables");
    }

    @Bean(name = "ddbSynapseTables")
    public Table ddbSynapseTables() {
        return ddbClient().getTable(ddbPrefix() + "SynapseTables");
    }

    @Bean(name = "ddbRecordTable")
    public Table ddbRecordTable() {
        return ddbClient().getTable(ddbPrefix() + "HealthDataRecord3");
    }

    @Bean
    public DynamoScanHelper ddbScanHelper() {
        return new DynamoScanHelper();
    }
}
