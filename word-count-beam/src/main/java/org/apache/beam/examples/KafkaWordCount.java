package org.apache.beam.examples;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;


public class KafkaWordCount {

    public interface KafkaWordCountOptions extends PipelineOptions {

        @Description("Kafka bootstrap server")
        @Default.String("kafka:9092")
        String getBootstrapServer();

        void setBootstrapServer(String value);

        /**
         * By default, this example reads from words topic
         */
        @Description("Topic to read from ")
        @Default.String("words")
        String getInputTopic();

        void setInputTopic(String value);

        /** Set this required option to specify where to write the output.
         *  Default is word-count
         */
        @Description("Topic to send count of words")
        @Default.String("word-count")
        String getOutputTopic();

        void setOutputTopic(String value);
    }

    static void runWordCount(KafkaWordCountOptions options) {

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Long>> counts = p.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(options.getBootstrapServer())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"latest"))
                .withoutMetadata() // PCollection<KV<Long, String>> instead of KafkaRecord type
        )
                .apply(
                        "FixedWindow",
                        Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(1)))
                                 .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1))))
                                 .accumulatingFiredPanes()
                                .withAllowedLateness(Duration.standardSeconds(0))
                )
                .apply(Values.<String>create())
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @DoFn.ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split("\\s+")) {
                            if (!word.isEmpty()) {
                                c.output(word.toLowerCase());
                            }
                        }
                    }
                }))
                .apply(Count.<String>perElement());
        // Output to Kafka Topic
        counts
                .apply(KafkaIO.<String, Long>write()
                        .withBootstrapServers(options.getBootstrapServer())
                        .withTopic(options.getOutputTopic())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(LongSerializer.class)
                );

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        KafkaWordCountOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaWordCountOptions.class);

        runWordCount(options);
    }
}
