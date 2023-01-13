package co.elastic.apm.agent.plugins.s3;

import co.elastic.apm.agent.sdk.ElasticApmInstrumentation;
import io.opentelemetry.api.trace.Span;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import software.amazon.awssdk.core.SdkRequest;

import java.util.Collection;
import java.util.Collections;

import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

public class S3FileNameInstrumentation extends ElasticApmInstrumentation {

    private static final String OBJECT_KEY_FIELD_NAME = "Key";
    private static final String BUCKET_FIELD_NAME = "Bucket";

    @Override
    public ElementMatcher<? super TypeDescription> getTypeMatcher() {
        return named("software.amazon.awssdk.core.internal.handler.BaseSyncClientHandler");
    }

    @Override
    public ElementMatcher<? super MethodDescription> getMethodMatcher() {
        return nameStartsWith("invoke")
                .and(takesArgument(0, named("software.amazon.awssdk.http.SdkHttpFullRequest")))
                .and(takesArgument(1, named("software.amazon.awssdk.core.SdkRequest")));
    }

    @Override
    public Collection<String> getInstrumentationGroupNames() {
        return Collections.singleton("s3-filename");
    }

    @Override
    public String getAdviceClassName() {
        return "co.elastic.apm.agent.plugins.s3.S3FileNameInstrumentation$AdviceClass";
    }

    public static class AdviceClass {
        @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
        public static void onEnterHandle(@Advice.Argument(1) SdkRequest sdkRequest) {
            String objectKey = sdkRequest.getValueForField(OBJECT_KEY_FIELD_NAME, String.class).orElse(null);
            String bucketName = sdkRequest.getValueForField(BUCKET_FIELD_NAME, String.class).orElse(null);
            if (null != objectKey) {
                Span.current().setAttribute("s3.object_key", objectKey);
            }
            if (null != bucketName) {
                Span.current().setAttribute("s3.bucket_name", bucketName);
            }
        }
    }
}
