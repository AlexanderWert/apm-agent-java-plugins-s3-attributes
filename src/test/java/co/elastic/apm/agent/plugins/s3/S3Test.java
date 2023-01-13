package co.elastic.apm.agent.plugins.s3;

import co.elastic.apm.agent.plugins.s3.plugin.AbstractInstrumentationTest;
import com.fasterxml.jackson.databind.JsonNode;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class S3Test extends AbstractInstrumentationTest {
    protected static final String BUCKET_NAME = "some-test-bucket";
    protected static final String OBJECT_KEY = "some-object-key";

    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:0.14.2");
    @Container
    protected LocalStackContainer localstack = new LocalStackContainer(localstackImage).withServices(LocalStackContainer.Service.S3);

    S3Client s3;
    S3AsyncClient s3Async;

    static {
        setProperty("elastic.apm.log_level", "DEBUG");
    }

    @BeforeEach
    public void setupClient() {
        s3 = S3Client.builder().endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey()
                )))
                .region(Region.of(localstack.getRegion())).build();
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());

        s3Async = S3AsyncClient.builder().endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey()
                )))
                .region(Region.of(localstack.getRegion())).build();
    }

    @Test
    public void test() throws TimeoutException {
        Tracer elasticTracer = GlobalOpenTelemetry.get().getTracer("S3Test");
        Span span = elasticTracer.spanBuilder("Transaction").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET_NAME).key(OBJECT_KEY).build(), RequestBody.fromString("This is some Object content"));
        } finally {
            span.end();
        }

        JsonNode observedSpan = ApmServer.getAndRemoveSpan(0, 1000);
        assertEquals(BUCKET_NAME, observedSpan.get("otel").get("attributes").get("s3.bucket_name").asText());
        assertEquals(OBJECT_KEY, observedSpan.get("otel").get("attributes").get("s3.object_key").asText());
    }
}
