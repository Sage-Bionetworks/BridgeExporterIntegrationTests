package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.HealthDataApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.UploadSchemasApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.HealthDataRecord;
import org.sagebionetworks.bridge.rest.model.HealthDataSubmission;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;

public class ExportTest {
    private static final Logger LOG = LoggerFactory.getLogger(ExportTest.class);

    private static final String CONFIG_FILE = "BridgeExporter-test.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    private static final String CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET = "record.id.override.bucket";
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_STUDY_ID = "api";

    private static final String APP_VERSION = "version 1.0.2, build 2";
    private static final String CREATED_ON_STR = "2015-04-02T03:27:09-07:00";
    private static final DateTime CREATED_ON = DateTime.parse(CREATED_ON_STR);
    private static final String CREATED_ON_TIME_ZONE = "-0700";
    private static final String METADATA_FIELD_NAME = "integTestRunId";
    private static final int METADATA_FIELD_LENGTH = 4;
    private static final String METADATA_SYNAPSE_COLUMN_NAME = "metadata." + METADATA_FIELD_NAME;
    private static final String PHONE_INFO = "BridgeEXIntegTest";
    private static final String SCHEMA_ID = "legacy-survey";
    private static final long SCHEMA_REV = 1;
    private static final int EXPORT_SINGLE_SECONDS = 10;
    private static final int SYNAPSE_SECONDS = 1;

    private static final Set<String> COMMON_COLUMN_NAME_SET = ImmutableSet.of("recordId", "appVersion", "phoneInfo",
            "uploadDate", "healthCode", "externalId", "dataGroups", "createdOn", "createdOnTimeZone",
            "userSharingScope");
    private static final Set<String> SURVEY_COLUMN_NAME_SET = ImmutableSet.of(METADATA_SYNAPSE_COLUMN_NAME, "AAA",
            "BBB.fencing", "BBB.football", "BBB.running", "BBB.swimming", "BBB.3");

    // Retry up to 6 times, so we don't spend more than 30 seconds per test.
    private static final int EXPORT_RETRIES = 6;
    private static final int SYNAPSE_RETRIES = 9;

    private static final String USER_NAME = "synapse.user";
    private static final String SYNAPSE_API_KEY_NAME = "synapse.api.key";

    // services
    private static AmazonS3Client s3Client;
    private static S3Helper s3Helper;
    private static SqsHelper sqsHelper;
    private static SynapseClient synapseClient;

    private static String exporterSqsUrl;
    private static String recordIdOverrideBucket;

    // timestamps
    private static DateTime endDateTime;
    private static DateTime now;
    private static DateTime startDateTime;
    private static DateTime uploadDateTime;

    // misc
    private static ExecutorService executorService;
    private static String integTestRunId;
    private static Table ddbExportTimeTable;
    private static Table ddbSynapseMetaTables;
    private static Table ddbSynapseTables;
    private static Table ddbRecordTable;
    private static TestUserHelper.TestUser admin;
    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser user;

    // per-test values
    private String recordId;
    private String s3FileName;

    // We want to only set up everything once for the entire test suite, not before each individual test. This means
    // using @BeforeClass, which unfortunately prevents us from using Spring.
    @BeforeClass
    public static void beforeClass() throws Exception {
        // bridge config
        //noinspection ConstantConditions
        String defaultConfig = ExportTest.class.getClassLoader().getResource(DEFAULT_CONFIG_FILE).getPath();
        Path defaultConfigPath = Paths.get(defaultConfig);
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        Config bridgeConfig;
        if (Files.exists(localConfigPath)) {
            bridgeConfig = new PropertiesConfig(defaultConfigPath, localConfigPath);
        } else {
            bridgeConfig = new PropertiesConfig(defaultConfigPath);
        }

        // config vars
        Environment env = bridgeConfig.getEnvironment();
        exporterSqsUrl = bridgeConfig.get("exporter.request.sqs.queue.url");
        recordIdOverrideBucket = bridgeConfig.get(CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
        String synapseUser = bridgeConfig.get(USER_NAME);
        String synapseApiKey = bridgeConfig.get(SYNAPSE_API_KEY_NAME);

        // AWS services
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(bridgeConfig.get("aws.key"),
                bridgeConfig.get("aws.secret.key"));

        s3Client = new AmazonS3Client(awsCredentials);
        s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client);

        sqsHelper = new SqsHelper();
        sqsHelper.setSqsClient(new AmazonSQSClient(awsCredentials));

        // DDB tables
        DynamoDB ddbClient = new DynamoDB(new AmazonDynamoDBClient(awsCredentials));

        String ddbBridgePrefix = env.name().toLowerCase() + '-' + bridgeConfig.getUser() + '-';
        String ddbExporterPrefix = bridgeConfig.get("exporter.ddb.prefix");

        ddbExportTimeTable = ddbClient.getTable(ddbBridgePrefix + "ExportTime");
        ddbRecordTable = ddbClient.getTable(ddbBridgePrefix + "HealthDataRecord3");
        ddbSynapseMetaTables = ddbClient.getTable(ddbExporterPrefix + "SynapseMetaTables");
        ddbSynapseTables = ddbClient.getTable(ddbExporterPrefix + "SynapseTables");

        // Bridge clients
        admin = TestUserHelper.getSignedInAdmin();
        developer = TestUserHelper.createAndSignInUser(ExportTest.class, TEST_STUDY_ID, false, Role.DEVELOPER);
        user = TestUserHelper.createAndSignInUser(ExportTest.class, TEST_STUDY_ID, true);

        // Synapse clients
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);

        // ensure we have a metadata field in the study
        StudiesApi studiesApi = developer.getClient(StudiesApi.class);
        Study study = studiesApi.getUsersStudy().execute().body();
        List<UploadFieldDefinition> metadataFieldDefList = study.getUploadMetadataFieldDefinitions();

        // Find the metadata field.
        boolean foundMetadataField = false;
        for (UploadFieldDefinition oneFieldDef : metadataFieldDefList) {
            if (METADATA_FIELD_NAME.equals(oneFieldDef.getName())) {
                foundMetadataField = true;
                break;
            }
        }

        if (!foundMetadataField) {
            // No metadata field. Create it.
            UploadFieldDefinition metadataField = new UploadFieldDefinition().name(METADATA_FIELD_NAME)
                    .type(UploadFieldType.STRING).maxLength(METADATA_FIELD_LENGTH);
            study.addUploadMetadataFieldDefinitionsItem(metadataField);
            studiesApi.updateUsersStudy(study).execute();
        }

        // ensure schemas exist, so we have something to upload against
        UploadSchemasApi uploadSchemasApi = developer.getClient(UploadSchemasApi.class);

        UploadSchema legacySurveySchema = null;
        try {
            legacySurveySchema = uploadSchemasApi.getMostRecentUploadSchema(SCHEMA_ID).execute().body();
        } catch (EntityNotFoundException ex) {
            // no-op
        }
        if (legacySurveySchema == null) {
            UploadFieldDefinition def1 = new UploadFieldDefinition();
            def1.setName("AAA");
            def1.setType(UploadFieldType.SINGLE_CHOICE);

            UploadFieldDefinition def2 = new UploadFieldDefinition();
            def2.setName("BBB");
            def2.setAllowOtherChoices(Boolean.FALSE);
            def2.setType(UploadFieldType.MULTI_CHOICE);
            def2.setMultiChoiceAnswerList(Lists.newArrayList("fencing", "football", "running", "swimming", "3"));

            legacySurveySchema = new UploadSchema();
            legacySurveySchema.setSchemaId(SCHEMA_ID);
            legacySurveySchema.setRevision(SCHEMA_REV);
            legacySurveySchema.setName("Legacy (RK/AC) Survey");
            legacySurveySchema.setSchemaType(UploadSchemaType.IOS_SURVEY);
            legacySurveySchema.setFieldDefinitions(Lists.newArrayList(def1,def2));
            uploadSchemasApi.createUploadSchema(legacySurveySchema).execute();
        }

        // make the user account sharing upload to enable export
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        StudyParticipant userParticipant = usersApi.getUsersParticipantRecord().execute().body();
        userParticipant.sharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        usersApi.updateUsersParticipantRecord(userParticipant).execute().body();

        // instantiate executor
        executorService = Executors.newCachedThreadPool();

        // Take a snapshot of "now" so we don't get weird clock drift while the test is running.
        now = DateTime.now();

        // Clock skew on our Jenkins machine can be up to 5 minutes. Because of this, set the upload's upload time to
        // 10 min ago, and export in a window between 15 min ago and 5 min ago.
        startDateTime = now.minusMinutes(15);
        uploadDateTime = now.minusMinutes(10);
        endDateTime = now.minusMinutes(5);

        // Generate a test run ID
        integTestRunId = RandomStringUtils.randomAlphabetic(METADATA_FIELD_LENGTH);
        LOG.info("integTestRunId=" + integTestRunId);
    }

    @BeforeMethod
    public void before() throws Exception {
        // Submit health data - Note that we build maps, since Jackson and GSON don't mix very well.
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("AAA", "Yes");
        dataMap.put("BBB", ImmutableSet.of("fencing", "running", 3));

        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(METADATA_FIELD_NAME, integTestRunId);

        HealthDataSubmission submission = new HealthDataSubmission().appVersion(APP_VERSION).createdOn(CREATED_ON)
                .phoneInfo(PHONE_INFO).schemaId(SCHEMA_ID).schemaRevision(SCHEMA_REV).metadata(metadataMap)
                .data(dataMap);

        HealthDataApi healthDataApi = user.getClient(HealthDataApi.class);
        HealthDataRecord record = healthDataApi.submitHealthData(submission).execute().body();
        recordId = record.getId();

        // set uploadedOn to 10 min ago.
        ddbRecordTable.updateItem("id", recordId, new AttributeUpdate("uploadedOn").put(uploadDateTime.getMillis()));
    }

    @AfterMethod
    public void after() {
        if (StringUtils.isNotBlank(s3FileName)) {
            s3Client.deleteObject(recordIdOverrideBucket, s3FileName);
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }

        if (developer != null) {
            developer.signOutAndDeleteUser();
        }

        if (user != null) {
            user.signOutAndDeleteUser();
        }

        if (admin != null) {
            admin.signOut();
        }
    }

    @Test
    public void useLastExportTime() throws Exception {
        LOG.info("Starting useLastExportTime() for record " + recordId);

        // set exportTime to 15 min ago
        ddbExportTimeTable.updateItem("studyId", TEST_STUDY_ID, new AttributeUpdate("lastExportDateTime").put(
                startDateTime.getMillis()));

        // create request - endDateTime is 5 minutes ago
        String requestText = "{\n" +
                "   \"endDateTime\":\"" + endDateTime.toString() + "\",\n" +
                "   \"useLastExportTime\":true,\n" +
                "   \"tag\":\"EX Integ Test - useLastExportTime 1\"\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestText);
        executeAndValidate(requestNode, 1);

        // Second request with endDateTime at "now". We don't export to the same table. But the old record is there
        // from the previous export. Therefore, we expect 1 copy of that upload.
        String request2Text = "{\n" +
                "   \"endDateTime\":\"" + now.toString() + "\",\n" +
                "   \"useLastExportTime\":true,\n" +
                "   \"tag\":\"EX Integ Test - useLastExportTime 2\"\n" +
                "}";
        ObjectNode request2Node = (ObjectNode) JSON_OBJECT_MAPPER.readTree(request2Text);
        executeAndValidate(request2Node, 1);
    }

    @Test
    public void startAndEndDateTime() throws Exception {
        LOG.info("Starting startAndEndDateTime() for record " + recordId);

        // create request - startDateTime is 15 minutes ago, endDateTime is 5 minutes ago
        String requestText = "{\n" +
                "   \"startDateTime\":\"" + startDateTime.toString() + "\",\n" +
                "   \"endDateTime\":\"" + endDateTime.toString() + "\",\n" +
                "   \"useLastExportTime\":false,\n" +
                "   \"tag\":\"EX Integ Test - start/endDateTime\"\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestText);
        executeAndValidate(requestNode, 1);

        // Issue the same request again. Exporter honors start/endDateTime, so we export the upload again.
        executeAndValidate(requestNode, 2);
    }

    @Test
    public void s3Override() throws Exception {
        LOG.info("Starting s3Override() for record " + recordId);

        // upload the override file to S3
        s3FileName = "ex-integ-test-record-ids." + now.toString();
        s3Helper.writeLinesToS3(recordIdOverrideBucket, s3FileName, ImmutableList.of(recordId));

        // create request
        String requestText = "{\n" +
                "   \"recordIdS3Override\":\"" + s3FileName + "\",\n" +
                "   \"useLastExportTime\":false,\n" +
                "   \"tag\":\"EX Integ Test - s3 override\"\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestText);
        executeAndValidate(requestNode, 1);

        // Issue the same request again. We export the upload again.
        executeAndValidate(requestNode, 2);
    }

    private void executeAndValidate(ObjectNode requestNode, int expectedUploadCount) throws Exception {
        // enforce study whitelist, to make sure we don't export outside of the API study
        ArrayNode studyWhitelistArray = JSON_OBJECT_MAPPER.createArrayNode();
        studyWhitelistArray.add(TEST_STUDY_ID);
        requestNode.set("studyWhitelist", studyWhitelistArray);

        LOG.info("Time before request exporting: " + DateTime.now().toString());

        // execute
        sqsHelper.sendMessageAsJson(exporterSqsUrl, requestNode, 0);

        // verification
        // first verify export time table since it is the last step in exporter -- if its done, other tasks should be
        // done as well
        boolean useLastExportTime = requestNode.get("useLastExportTime").booleanValue();
        if (useLastExportTime) {
            verifyExportTime(DateTime.parse(requestNode.get("endDateTime").textValue()).getMillis());
        } else {
            // todo Wait for the Exporter to finish.
            // Until https://sagebionetworks.jira.com/browse/BRIDGE-1826 is implemented, we have no way of knowing for
            // sure that the Exporter is finished. Until that's implemented, wait about 30 seconds.
            TimeUnit.SECONDS.sleep(30);
        }
        verifyExport(expectedUploadCount);
    }

    private void verifyExportTime(long endDateTimeEpoch) throws Exception {
        Item lastExportDateTime;
        Long lastExportDateTimeEpoch = null;

        for (int i = 0; i < EXPORT_RETRIES; i++) {
            lastExportDateTime = ddbExportTimeTable.getItem("studyId", TEST_STUDY_ID);
            if (lastExportDateTime != null) {
                lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
                if (endDateTimeEpoch == lastExportDateTimeEpoch) {
                    break;
                }
            }

            LOG.info("Retry get last export date time times: " + i);
            TimeUnit.SECONDS.sleep(EXPORT_SINGLE_SECONDS);
        }

        // the time recorded in export time table should be equal to the date time we submit to the export request
        assertNotNull(lastExportDateTimeEpoch);
        assertEquals(lastExportDateTimeEpoch.longValue(), endDateTimeEpoch);
    }

    private void verifyExport(int expectedUploadCount) throws Exception {
        // Kick off synapse queries in table, so we can minimize idle wait.

        // query appVersion table
        Item appVersionItem = ddbSynapseMetaTables.getItem("tableName", TEST_STUDY_ID + "-appVersion");
        final String appVersionTableId = appVersionItem.getString("tableId");
        assertFalse(StringUtils.isBlank(appVersionTableId));
        Future<RowSet> appVersionFuture = executorService.submit(() -> querySynapseTable(appVersionTableId, recordId,
                expectedUploadCount));

        // query survey result table
        Item surveyItem = ddbSynapseTables.getItem("schemaKey", TEST_STUDY_ID + "-legacy-survey-v1");
        final String surveyTableId = surveyItem.getString("tableId");
        assertFalse(StringUtils.isBlank(surveyTableId));
        Future<RowSet> surveyFuture = executorService.submit(() -> querySynapseTable(surveyTableId, recordId,
                expectedUploadCount));

        // wait for table results
        RowSet appVersionRowSet = appVersionFuture.get();
        RowSet surveyRowSet = surveyFuture.get();

        // Get column metadata. This helps with validation.
        Item uploadRecord = ddbRecordTable.getItem("id", recordId);

        // We've already validated row count. Just get the headers and the first row.

        // Validate appVersion result.
        List<SelectColumn> appVersionHeaderList = appVersionRowSet.getHeaders();
        List<String> appVersionColumnList = appVersionRowSet.getRows().get(0).getValues();
        Set<String> appVersionColumnNameSet = new HashSet<>();
        for (int i = 0; i < appVersionHeaderList.size(); i++) {
            String headerName = appVersionHeaderList.get(i).getName();
            String columnValue = appVersionColumnList.get(i);
            appVersionColumnNameSet.add(headerName);

            if (headerName.equals("originalTable")) {
                assertEquals(columnValue, "legacy-survey-v1");
            } else {
                commonColumnsVerification(headerName, columnValue, recordId, uploadRecord);
            }
        }
        assertTrue(appVersionColumnNameSet.containsAll(COMMON_COLUMN_NAME_SET));
        assertTrue(appVersionColumnNameSet.contains("originalTable"));

        List<SelectColumn> surveyHeaderList = surveyRowSet.getHeaders();
        List<String> surveyColumnList = surveyRowSet.getRows().get(0).getValues();

        // verify each column value
        Set<String> surveyColumnNameSet = new HashSet<>();
        for (int i = 0; i < surveyHeaderList.size(); i++) {
            String headerName = surveyHeaderList.get(i).getName();
            String columnValue = surveyColumnList.get(i);
            surveyColumnNameSet.add(headerName);

            switch (headerName) {
                case METADATA_SYNAPSE_COLUMN_NAME: {
                    assertEquals(columnValue, integTestRunId);
                    break;
                }
                case "AAA": {
                    assertEquals(columnValue, "Yes");
                    break;
                }
                case "BBB.fencing": {
                    assertEquals(columnValue, "true");
                    break;
                }
                case "BBB.football": {
                    assertEquals(columnValue, "false");
                    break;
                }
                case "BBB.running": {
                    assertEquals(columnValue, "true");
                    break;
                }
                case "BBB.swimming": {
                    assertEquals(columnValue, "false");
                    break;
                }
                case "BBB.3": {
                    assertEquals(columnValue, "true");
                    break;
                }
                default: commonColumnsVerification(headerName, columnValue, recordId, uploadRecord);
            }
        }
        assertTrue(surveyColumnNameSet.containsAll(COMMON_COLUMN_NAME_SET));
        assertTrue(surveyColumnNameSet.containsAll(SURVEY_COLUMN_NAME_SET));
    }

    private RowSet querySynapseTable(String tableId, String expectedRecordId, int expectedCount) throws Exception {
        // Query Synapse
        // Set limit to expectedCount+1. That way, if there are more than we expect, we'll know.
        String jobIdToken = synapseClient.queryTableEntityBundleAsyncStart("select * from " + tableId +
                " where recordId='" + expectedRecordId + "'", 0L, expectedCount+1L, true, 0xF, tableId);

        QueryResultBundle queryResultBundle = null;
        for (int j = 0; j < SYNAPSE_RETRIES; j++) {
            try {
                LOG.info("Retry get synapse table query result times: " + j);
                queryResultBundle = synapseClient.queryTableEntityBundleAsyncGet(jobIdToken, tableId);
                break;
            } catch (SynapseResultNotReadyException e) {
                TimeUnit.SECONDS.sleep(SYNAPSE_SECONDS);
            }
        }
        assertNotNull(queryResultBundle);

        // count rows
        RowSet rowSet = queryResultBundle.getQueryResult().getQueryResults();
        assertEquals(rowSet.getRows().size(), expectedCount);
        return rowSet;
    }

    private void commonColumnsVerification(String headerName, String columnValue, String expectedRecordId,
            Item uploadRecord) {
        switch (headerName) {
            case "recordId": {
                assertEquals(columnValue, expectedRecordId);
                break;
            }
            case "appVersion": {
                assertEquals(columnValue, APP_VERSION);
                break;
            }
            case "phoneInfo": {
                assertEquals(columnValue, PHONE_INFO);
                break;
            }
            case "uploadDate": {
                assertEquals(columnValue, uploadRecord.getString("uploadDate"));
                break;
            }
            case "healthCode": {
                assertEquals(columnValue, uploadRecord.getString("healthCode"));
                break;
            }
            case "externalId": {
                assertTrue(StringUtils.isBlank(columnValue));
                break;
            }
            case "dataGroups": {
                assertTrue(StringUtils.isBlank(columnValue));
                break;
            }
            case "createdOn": {
                assertEquals(columnValue, String.valueOf(CREATED_ON.getMillis()));
                break;
            }
            case "createdOnTimeZone": {
                assertEquals(columnValue, CREATED_ON_TIME_ZONE);
                break;
            }
            case "userSharingScope": {
                assertEquals(columnValue, uploadRecord.get("userSharingScope"));
                break;
            }
            default:
                LOG.info("Un-recognized column " + headerName + " added to synapse.");
        }
    }
}
