package com.nagarro.nagp.document_upload_lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;

public class S3UploadProcessorHandler implements RequestHandler<S3Event, String> {

    private final S3Client s3Client;
    private final SecretsManagerClient secretsManagerClient;
    private final ObjectMapper objectMapper;

    private final String dbHost;
    private final String dbPort;
    private final String dbName;
    private final String dbTableName;
    private final String dbSecretArn;

    private record DatabaseCredentials(String username, String password) {}

    public S3UploadProcessorHandler() {
        String awsRegion = System.getenv("AWS_REGION");
        if (awsRegion == null || awsRegion.isBlank()) {
            throw new IllegalStateException("AWS_REGION environment variable is not available");
        }
        this.s3Client = S3Client.builder().region(Region.of(awsRegion)).build();
        this.secretsManagerClient = SecretsManagerClient.builder().region(Region.of(awsRegion)).build();
        this.objectMapper = new ObjectMapper();
        this.dbHost = getRequiredEnvironmentVariable("DB_HOST");
        this.dbPort = getEnvironmentVariable("DB_PORT", "3306");
        this.dbName = getRequiredEnvironmentVariable("DB_NAME");
        this.dbTableName = getEnvironmentVariable("DB_TABLE_NAME", "uploaded_files");
        this.dbSecretArn = getRequiredEnvironmentVariable("DB_SECRET_ARN");
    }

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        if (s3Event == null || s3Event.getRecords() == null) {
            context.getLogger().log("Received empty S3 event");
            return "No records to process";
        }
        int processedCount = 0;
        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            try {
                String bucketName = record.getS3().getBucket().getName();
                String encodedObjectKey = record.getS3().getObject().getKey();
                String objectKey = URLDecoder.decode(encodedObjectKey, StandardCharsets.UTF_8);
                DateTime eventTime = record.getEventTime();
                context.getLogger().log("Processing S3 object");
                context.getLogger().log("Bucket: " + bucketName);
                context.getLogger().log("Key: " + objectKey);
                context.getLogger().log("Step 1: Reading S3 object metadata...");

                /*
                 * Read the object's metadata.
                 *
                 * HEAD Object returns metadata such as: Content-Type, Content-Length, Last-Modified, Etag
                 */
                HeadObjectRequest headObjectRequest =
                        HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(objectKey)
                        .build();

                context.getLogger().log("About to call S3 HeadObject");

//                HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
                HeadObjectResponse headObjectResponse;

                try {
                    context.getLogger().log("Calling S3 HeadObject...");
                    headObjectResponse = s3Client.headObject(headObjectRequest);
                    context.getLogger().log("S3 HeadObject call completed.");
                } catch (Exception e) {
                    context.getLogger().log("S3 HeadObject FAILED.");
                    context.getLogger().log("Exception class: " + e.getClass().getName());
                    context.getLogger().log("Exception message: " + String.valueOf(e.getMessage()));
                    Throwable cause = e.getCause();
                    while (cause != null) {
                        context.getLogger().log("Cause: " +
                                cause.getClass().getName() + " - " + String.valueOf(cause.getMessage()));
                        cause = cause.getCause();
                    }
                    throw e;
                }

                String contentType = headObjectResponse.contentType();

                context.getLogger().log("Step 2: S3 object metadata retrieved successfully.");
                context.getLogger().log("Content-Type: " + contentType);

                if (contentType == null || contentType.isBlank()) {
                    contentType = "application/octet-stream";
                }

                DateTime uploadedTimestamp = eventTime;
                if (uploadedTimestamp == null) {
                    uploadedTimestamp = DateTime.now();
                }

                context.getLogger().log("Step 3: Reading database credentials from Secrets Manager...");

                DatabaseCredentials credentials = getDatabaseCredentials();

                context.getLogger().log("Step 4: Database credentials retrieved successfully.");
                context.getLogger().log("Step 5: Connecting to RDS and inserting metadata...");

                insertUploadMetadata(objectKey, contentType, uploadedTimestamp, credentials);

                context.getLogger().log("Step 6: Metadata inserted successfully into RDS.");
                context.getLogger().log(
                        "Successfully inserted metadata. " +
                                "FileName=" + objectKey +
                                ", ContentType=" + contentType +
                                ", UploadTimestamp=" + uploadedTimestamp
                );
                processedCount += 1;
            } catch (Exception e) {
                context.getLogger().log("Failed to process S3 record");
                context.getLogger().log("Exception type: " + e.getClass().getName());
                context.getLogger().log("Exception message: " + e.getMessage());
                if (e.getCause() != null) {
                    context.getLogger().log("Cause class: " + e.getCause().getClass().getName());
                    context.getLogger().log("Cause message: " + e.getCause().getMessage());
                }
                throw new RuntimeException("Failed to process S3 upload", e);
            }
        }
        context.getLogger().log(
                "Lambda processing completed successfully. " +
                        "Processed records: " + processedCount
        );
        return "Processed " + processedCount + " S3 record(s)";
    }

    private DatabaseCredentials getDatabaseCredentials() throws JsonProcessingException {
        GetSecretValueRequest request =
                GetSecretValueRequest.builder()
                        .secretId(dbSecretArn)
                        .build();
        GetSecretValueResponse response = secretsManagerClient.getSecretValue(request);
        String secretString = response.secretString();

        if (secretString == null || secretString.isBlank()) {
            throw new IllegalStateException("Database secret does not contain a secret string");
        }

        JsonNode secretJson = objectMapper.readTree(secretString);
        JsonNode usernameNode = secretJson.get("username");
        JsonNode passwordNode = secretJson.get("password");

        if (usernameNode == null || passwordNode == null) {
            throw new IllegalStateException(
                    "Database secret must contain " + "'username' and 'password'");
        }

        return new DatabaseCredentials(usernameNode.asText(), passwordNode.asText());
    }

    private void insertUploadMetadata(
            String fileName,
            String contentType,
            DateTime uploadTimestamp,
            DatabaseCredentials credentials)
            throws SQLException {

        String jdbcUrl =
                "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName +
                        "?useSSL=true" + "&requireSSL=true" + "&serverTimezone=UTC";
        String sql =
                "INSERT INTO " + dbTableName +
                        " (file_name, content_type, upload_timestamp) " +
                        "VALUES (?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, credentials.username(), credentials.password());
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, fileName);
            statement.setString(2, contentType);
            statement.setTimestamp(3, Timestamp.from(Instant.ofEpochMilli(uploadTimestamp.getMillis())));
            int rowsInserted = statement.executeUpdate();
            if (rowsInserted != 1) {
                throw new SQLException("Expected 1 inserted row but got " + rowsInserted);
            }
        }
    }

    private String getRequiredEnvironmentVariable(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable is missing: " + name);
        }
        return value;
    }

    private String getEnvironmentVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }
}
