package org.sagebionetworks.bridge.exporter.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;
import org.sagebionetworks.repo.model.file.FileHandleAssociation;
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
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.HealthDataApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.SubstudiesApi;
import org.sagebionetworks.bridge.rest.api.UploadSchemasApi;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.HealthDataRecord;
import org.sagebionetworks.bridge.rest.model.HealthDataSubmission;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Substudy;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.rest.model.VersionHolder;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;

@SuppressWarnings({ "deprecation", "ResultOfMethodCallIgnored" })
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
    private static final String LARGE_TEXT_ATTACHMENT_SCHEMA_ID = "large-text-attachment-test";
    private static final long LARGE_TEXT_ATTACHMENT_SCHEMA_REV = 1;
    private static final String LARGE_TEXT_ATTACHMENT_TABLE_NAME = LARGE_TEXT_ATTACHMENT_SCHEMA_ID + "-v" +
            LARGE_TEXT_ATTACHMENT_SCHEMA_REV;
    private static final Map<String, Object> LEGACY_SURVEY_DATA_MAP = ImmutableMap.<String, Object>builder()
            .put("AAA", "Yes")
            .put("BBB", ImmutableList.of("fencing", "running", 3))
            .build();
    private static final String METADATA_FIELD_NAME = "integTestRunId";
    private static final int METADATA_FIELD_LENGTH = 4;
    private static final String METADATA_SYNAPSE_COLUMN_NAME = "metadata." + METADATA_FIELD_NAME;
    private static final String PHONE_INFO = "BridgeEXIntegTest";
    private static final String RAW_DATA_COLUMN_NAME = "rawData";
    private static final String SCHEMA_ID = "legacy-survey";
    private static final long SCHEMA_REV = 1;
    private static final String SCHEMA_TABLE_NAME = SCHEMA_ID + "-v" + SCHEMA_REV;
    private static final int EXPORT_SINGLE_SECONDS = 10;
    private static final int SYNAPSE_SECONDS = 2;

    private static final Set<String> COMMON_COLUMN_NAME_SET = ImmutableSet.of("recordId", "appVersion", "phoneInfo",
            "uploadDate", "healthCode", "externalId", "dataGroups", "createdOn", "createdOnTimeZone",
            "userSharingScope", "substudyMemberships", "dayInStudy");
    private static final Map<String, String> LEGACY_SURVEY_EXPECTED_HEALTH_DATA_MAP = ImmutableMap.<String, String>builder()
            .put("AAA", "Yes").put("BBB.fencing", "true").put("BBB.football", "false").put("BBB.running", "true")
            .put("BBB.swimming", "false").put("BBB.3", "true").build();

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
    private static File tmpDir;
    private static String integTestRunId;
    private static Table ddbExportTimeTable;
    private static Table ddbSynapseMetaTables;
    private static Table ddbSynapseTables;
    private static Table ddbRecordTable;
    private static TestUserHelper.TestUser admin;
    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser user;

    private static String dataGroup;
    private static String substudyId;
    
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

        // Study
        StudiesApi studiesApi = developer.getClient(StudiesApi.class);
        Study study = studiesApi.getUsersStudy().execute().body();
        if (study.getDataGroups().isEmpty()) {
            dataGroup = "group1";
            study.setDataGroups(ImmutableList.of(dataGroup));
            VersionHolder version = studiesApi.updateUsersStudy(study).execute().body();
            study.setVersion(version.getVersion());
        } else {
            dataGroup = study.getDataGroups().get(0);            
        }
        
        // Substudy
        SubstudiesApi substudiesApi = admin.getClient(SubstudiesApi.class);
        
        // This should sign in the admin...
        List<Substudy> substudies = substudiesApi.getSubstudies(false).execute().body().getItems();
        if (substudies.isEmpty()) {
            substudyId = "substudyA";
            Substudy substudy = new Substudy().id(substudyId).name("Substudy " + substudyId);
            substudiesApi.createSubstudy(substudy).execute();
        } else {
            substudyId = substudies.get(0).getId();    
        }

        String userEmail = TestUserHelper.makeEmail(ExportTest.class);
        SignUp signUp = new SignUp().email(userEmail).password("P@ssword`1").study(TEST_STUDY_ID);
        signUp.dataGroups(ImmutableList.of(dataGroup));
        signUp.substudyIds(ImmutableList.of(substudyId));
        user = new TestUserHelper.Builder(ExportTest.class).withSignUp(signUp).withConsentUser(true)
                .createAndSignInUser(TEST_STUDY_ID);

        // Initialize user by asking for activities. This sets the activities_retrieved event, so we can calculate
        // dayInStudy.
        user.getClient(ForConsentedUsersApi.class).getScheduledActivitiesByDateRange(DateTime.now(),
                DateTime.now().plusDays(1)).execute();

        // Synapse clients
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUser);
        synapseClient.setApiKey(synapseApiKey);

        // ensure we have a metadata field in the study
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

        // legacy-survey schema, for integ tests
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

        // large-text-attachment-test schema
        UploadSchema largeTextAttachmentTestSchema = null;
        try {
            largeTextAttachmentTestSchema = uploadSchemasApi.getMostRecentUploadSchema(LARGE_TEXT_ATTACHMENT_SCHEMA_ID)
                    .execute().body();
        } catch (EntityNotFoundException ex) {
            // no-op
        }
        if (largeTextAttachmentTestSchema == null) {
            UploadFieldDefinition largeTextFieldDef = new UploadFieldDefinition().name("my-large-text-attachment")
                    .type(UploadFieldType.LARGE_TEXT_ATTACHMENT);
            largeTextAttachmentTestSchema = new UploadSchema().schemaId(LARGE_TEXT_ATTACHMENT_SCHEMA_ID)
                    .revision(LARGE_TEXT_ATTACHMENT_SCHEMA_REV).name("Large Text Attachment Test")
                    .schemaType(UploadSchemaType.IOS_DATA).addFieldDefinitionsItem(largeTextFieldDef);
            uploadSchemasApi.createUploadSchema(largeTextAttachmentTestSchema).execute();
        }

        // make the user account sharing upload to enable export
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        StudyParticipant userParticipant = usersApi.getUsersParticipantRecord().execute().body();
        userParticipant.sharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        usersApi.updateUsersParticipantRecord(userParticipant).execute().body();

        // instantiate executor
        executorService = Executors.newCachedThreadPool();

        // Make temp dir.
        tmpDir = com.google.common.io.Files.createTempDir();

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
    public void before() {
        // Reset recordId and s3Filename before each test (since it's not clear TestNG does).
        recordId = null;
        s3FileName = null;
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
            try {
                admin.signOut();    
            } catch(BadRequestException e) {
                // Ignore exception.
            }
        }
    }

    private void setupLegacySurveyTest() throws Exception {
        // Submit health data - Note that we build maps, since Jackson and GSON don't mix very well.
        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(METADATA_FIELD_NAME, integTestRunId);

        HealthDataSubmission submission = new HealthDataSubmission().appVersion(APP_VERSION).createdOn(CREATED_ON)
                .phoneInfo(PHONE_INFO).schemaId(SCHEMA_ID).schemaRevision(SCHEMA_REV).metadata(metadataMap)
                .data(LEGACY_SURVEY_DATA_MAP);

        HealthDataApi healthDataApi = user.getClient(HealthDataApi.class);
        HealthDataRecord record = healthDataApi.submitHealthData(submission).execute().body();
        recordId = record.getId();

        // set uploadedOn to 10 min ago.
        ddbRecordTable.updateItem("id", recordId, new AttributeUpdate("uploadedOn").put(uploadDateTime.getMillis()));
    }

    @Test
    public void useLastExportTime() throws Exception {
        setupLegacySurveyTest();
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

        Callable<String> dataTableIdProvider = getDataTableIdProviderForSchema(SCHEMA_ID, SCHEMA_REV);
        executeAndValidate(requestNode, 1, SCHEMA_TABLE_NAME, dataTableIdProvider,
                LEGACY_SURVEY_DATA_MAP, LEGACY_SURVEY_EXPECTED_HEALTH_DATA_MAP);

        // Second request with endDateTime at "now". We don't export to the same table. But the old record is there
        // from the previous export. Therefore, we expect 1 copy of that upload.
        String request2Text = "{\n" +
                "   \"endDateTime\":\"" + now.toString() + "\",\n" +
                "   \"useLastExportTime\":true,\n" +
                "   \"tag\":\"EX Integ Test - useLastExportTime 2\"\n" +
                "}";
        ObjectNode request2Node = (ObjectNode) JSON_OBJECT_MAPPER.readTree(request2Text);
        executeAndValidate(request2Node, 1, SCHEMA_TABLE_NAME, dataTableIdProvider,
                LEGACY_SURVEY_DATA_MAP, LEGACY_SURVEY_EXPECTED_HEALTH_DATA_MAP);
    }

    @Test
    public void startAndEndDateTime() throws Exception {
        setupLegacySurveyTest();
        LOG.info("Starting startAndEndDateTime() for record " + recordId);

        // create request - startDateTime is 15 minutes ago, endDateTime is 5 minutes ago
        String requestText = "{\n" +
                "   \"startDateTime\":\"" + startDateTime.toString() + "\",\n" +
                "   \"endDateTime\":\"" + endDateTime.toString() + "\",\n" +
                "   \"useLastExportTime\":false,\n" +
                "   \"tag\":\"EX Integ Test - start/endDateTime\"\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestText);

        Callable<String> dataTableIdProvider = getDataTableIdProviderForSchema(SCHEMA_ID, SCHEMA_REV);
        executeAndValidate(requestNode, 1, SCHEMA_TABLE_NAME, dataTableIdProvider,
                LEGACY_SURVEY_DATA_MAP, LEGACY_SURVEY_EXPECTED_HEALTH_DATA_MAP);

        // Issue the same request again. Exporter honors start/endDateTime, so we export the upload again.
        executeAndValidate(requestNode, 2, SCHEMA_TABLE_NAME, dataTableIdProvider,
                LEGACY_SURVEY_DATA_MAP, LEGACY_SURVEY_EXPECTED_HEALTH_DATA_MAP);
    }

    @Test
    public void s3Override() throws Exception {
        setupLegacySurveyTest();
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

        Callable<String> dataTableIdProvider = getDataTableIdProviderForSchema(SCHEMA_ID, SCHEMA_REV);
        executeAndValidate(requestNode, 1, SCHEMA_TABLE_NAME, dataTableIdProvider,
                LEGACY_SURVEY_DATA_MAP, LEGACY_SURVEY_EXPECTED_HEALTH_DATA_MAP);

        // Issue the same request again. We export the upload again.
        executeAndValidate(requestNode, 2, SCHEMA_TABLE_NAME, dataTableIdProvider,
                LEGACY_SURVEY_DATA_MAP, LEGACY_SURVEY_EXPECTED_HEALTH_DATA_MAP);
    }

    @Test
    public void largeTextAttachment() throws Exception {
        // Submit health data - Note that we build maps, since Jackson and GSON don't mix very well.
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("my-large-text-attachment", "This is my large text attachment");

        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(METADATA_FIELD_NAME, integTestRunId);

        HealthDataSubmission submission = new HealthDataSubmission().appVersion(APP_VERSION).createdOn(CREATED_ON)
                .phoneInfo(PHONE_INFO).schemaId(LARGE_TEXT_ATTACHMENT_SCHEMA_ID)
                .schemaRevision(LARGE_TEXT_ATTACHMENT_SCHEMA_REV).metadata(metadataMap).data(dataMap);

        HealthDataApi healthDataApi = user.getClient(HealthDataApi.class);
        HealthDataRecord record = healthDataApi.submitHealthData(submission).execute().body();
        recordId = record.getId();
        LOG.info("Starting largeTextAttachment() for record " + recordId);

        // set uploadedOn to 10 min ago.
        ddbRecordTable.updateItem("id", recordId, new AttributeUpdate("uploadedOn").put(uploadDateTime.getMillis()));

        // create request - startDateTime is 15 minutes ago, endDateTime is 5 minutes ago
        String requestText = "{\n" +
                "   \"startDateTime\":\"" + startDateTime.toString() + "\",\n" +
                "   \"endDateTime\":\"" + endDateTime.toString() + "\",\n" +
                "   \"useLastExportTime\":false,\n" +
                "   \"tag\":\"EX Integ Test - Large Text Attachments\"\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestText);

        // Make expected output. Note that the string is quoted, because JSON attachments are always written as JSON.
        // For strings, this means with quotes. Most of the time, this will be a non-issue, since most JSON attachments
        // are objects or arrays.
        Map<String, String> expectedOutputMap = new HashMap<>();
        expectedOutputMap.put("my-large-text-attachment", "\"This is my large text attachment\"");

        Callable<String> dataTableIdProvider = getDataTableIdProviderForSchema(LARGE_TEXT_ATTACHMENT_SCHEMA_ID,
                LARGE_TEXT_ATTACHMENT_SCHEMA_REV);
        executeAndValidate(requestNode, 1, LARGE_TEXT_ATTACHMENT_TABLE_NAME, dataTableIdProvider,
                dataMap, expectedOutputMap);
    }

    @Test
    public void schemalessHealthData() throws Exception {
        // Submit health data - Note that we build maps, since Jackson and GSON don't mix very well.
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("dummy-key", "dummy-value");

        Map<String, String> metadataMap = new HashMap<>();
        metadataMap.put(METADATA_FIELD_NAME, integTestRunId);

        // Null out schema ID and rev to ensure schemaless health data.
        HealthDataSubmission submission = new HealthDataSubmission().appVersion(APP_VERSION).createdOn(CREATED_ON)
                .phoneInfo(PHONE_INFO).schemaId(null).schemaRevision(null).metadata(metadataMap).data(dataMap);

        HealthDataApi healthDataApi = user.getClient(HealthDataApi.class);
        HealthDataRecord record = healthDataApi.submitHealthData(submission).execute().body();
        recordId = record.getId();
        LOG.info("Starting schemalessHealthData() for record " + recordId);

        // Set uploadedOn to 10 min ago.
        ddbRecordTable.updateItem("id", recordId, new AttributeUpdate("uploadedOn")
                .put(uploadDateTime.getMillis()));

        // Create request - startDateTime is 15 minutes ago, endDateTime is 5 minutes ago.
        String requestText = "{\n" +
                "   \"startDateTime\":\"" + startDateTime.toString() + "\",\n" +
                "   \"endDateTime\":\"" + endDateTime.toString() + "\",\n" +
                "   \"useLastExportTime\":false,\n" +
                "   \"tag\":\"EX Integ Test - Schemaless Health Data\"\n" +
                "}";
        ObjectNode requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestText);

        // Since this is schemaless, the expected health data will be an empty map.
        Map<String, String> expectedOutputMap = new HashMap<>();

        Callable<String> dataTableIdProvider = () -> {
            Item tableItem = ddbSynapseMetaTables.getItem("tableName",
                    TEST_STUDY_ID + "-default");
            return tableItem.getString("tableId");
        };
        executeAndValidate(requestNode, 1, "Default Health Data Record Table",
                dataTableIdProvider, dataMap, expectedOutputMap);
    }

    private static Callable<String> getDataTableIdProviderForSchema(String schemaId, long schemaRev) {
        return () -> {
            String expectedSchemaKey = TEST_STUDY_ID + "-" + schemaId + "-v" + schemaRev;
            Item tableItem = ddbSynapseTables.getItem("schemaKey", expectedSchemaKey);
            return tableItem.getString("tableId");
        };
    }

    private void executeAndValidate(ObjectNode requestNode, int expectedUploadCount, String expectedOriginalTable,
            Callable<String> dataTableIdProvider, Map<String, Object> submittedHealthDataMap,
            Map<String, String> expectedHealthDataMap) throws Exception {
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
            TimeUnit.SECONDS.sleep(45);
        }
        verifyExport(expectedUploadCount, expectedOriginalTable, dataTableIdProvider, submittedHealthDataMap,
                expectedHealthDataMap);
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

    private void verifyExport(int expectedUploadCount, String expectedOriginalTable, Callable<String> dataTableIdProvider,
            Map<String, Object> submittedHealthDataMap, Map<String, String> expectedHealthDataMap) throws Exception {
        // Kick off synapse queries in table, so we can minimize idle wait.

        // query appVersion table
        Item appVersionItem = ddbSynapseMetaTables.getItem("tableName", TEST_STUDY_ID + "-appVersion");
        final String appVersionTableId = appVersionItem.getString("tableId");
        assertFalse(StringUtils.isBlank(appVersionTableId));
        Future<RowSet> appVersionFuture = executorService.submit(() -> querySynapseTable(appVersionTableId, recordId,
                expectedUploadCount));

        // query health data table
        String healthDataTableId = dataTableIdProvider.call();
        assertTrue(StringUtils.isNotBlank(healthDataTableId));
        Future<RowSet> healthDataFuture = executorService.submit(() -> querySynapseTable(healthDataTableId, recordId,
                expectedUploadCount));

        // wait for table results
        RowSet appVersionRowSet = appVersionFuture.get();
        RowSet healthDataRowSet = healthDataFuture.get();

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
                assertEquals(columnValue, expectedOriginalTable);
            } else {
                commonColumnsVerification(headerName, columnValue, recordId, uploadRecord);
            }
        }
        assertTrue(appVersionColumnNameSet.containsAll(COMMON_COLUMN_NAME_SET));
        assertTrue(appVersionColumnNameSet.contains("originalTable"));

        List<SelectColumn> healthDataHeaderList = healthDataRowSet.getHeaders();
        List<String> healthDataColumnList = healthDataRowSet.getRows().get(0).getValues();

        // verify each column value
        Set<String> healthDataColumnNameSet = new HashSet<>();
        for (int i = 0; i < healthDataHeaderList.size(); i++) {
            String headerName = healthDataHeaderList.get(i).getName();
            String columnValue = healthDataColumnList.get(i);
            healthDataColumnNameSet.add(headerName);

            if (METADATA_SYNAPSE_COLUMN_NAME.equals(headerName)) {
                assertEquals(columnValue, integTestRunId);
            } else if (expectedHealthDataMap.containsKey(headerName)) {
                assertEquals(columnValue, expectedHealthDataMap.get(headerName));
            } else if (headerName.equals(RAW_DATA_COLUMN_NAME)) {
                // We need to use a file handle association to ensure we have permissions to get the file handle.
                FileHandleAssociation fileHandleAssociation = new FileHandleAssociation();
                fileHandleAssociation.setAssociateObjectId(healthDataTableId);
                fileHandleAssociation.setAssociateObjectType(FileHandleAssociateType.TableEntity);
                fileHandleAssociation.setFileHandleId(columnValue);

                String rawDataFilename = recordId + "-" + RandomStringUtils.randomAlphabetic(4) + "-raw.json";
                File rawDataFile = new File(tmpDir, rawDataFilename);

                synapseClient.downloadFile(fileHandleAssociation, rawDataFile);

                Map<String, Object> rawDataMap = DefaultObjectMapper.INSTANCE.readValue(rawDataFile,
                        DefaultObjectMapper.TYPE_REF_RAW_MAP);
                assertEquals(rawDataMap, submittedHealthDataMap);
            } else {
                commonColumnsVerification(headerName, columnValue, recordId, uploadRecord);
            }
        }
        assertTrue(healthDataColumnNameSet.containsAll(COMMON_COLUMN_NAME_SET));
        assertTrue(healthDataColumnNameSet.containsAll(expectedHealthDataMap.keySet()));
        assertTrue(healthDataColumnNameSet.contains(METADATA_SYNAPSE_COLUMN_NAME));
        assertTrue(healthDataColumnNameSet.contains(RAW_DATA_COLUMN_NAME));
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
                assertEquals(columnValue, dataGroup);
                break;
            }
            case "substudyMemberships": {
                assertEquals(columnValue, String.format("|%s=|", substudyId));
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
            case "dayInStudy": {
                // Since the user was just created, this is always 1.
                assertEquals(columnValue, "1");
                break;
            }
            default:
                LOG.info("Un-recognized column " + headerName + " added to synapse.");
        }
    }
}
