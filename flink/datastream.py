from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Encoder

# import libraries for write
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy, BucketAssigner



# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()
# the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
# Enabling Checkpoint

class SourceData(object):
    def __init__(self, env):
        self.env = env
        self.env.add_jars("file:///Users/daniel.ford/Documents/GitHub/danielfordfc/python-playground/flink/flink-1.17.2/lib/flink-sql-connector-kafka-1.17.2.jar", \
                          "file:////Users/daniel.ford/Documents/GitHub/danielfordfc/python-playground/flink/flink-1.17.2/plugins/s3/flink-s3-fs-hadoop-1.17.2.jar")
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(1)
        self.env.enable_checkpointing(5*1000) # 30 seconds


    def get_data(self):
        source = KafkaSource.builder() \
            .set_bootstrap_servers("localhost:9092") \
            .set_topics("flink-test-1") \
            .set_group_id("my-group") \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        #  .set_starting_offsets(KafkaOffsetsInitializer.) \
        # .set_bounded(KafkaOffsetsInitializer.latest()) \


        # #output_path = f's3a://<bucket-name>/output/'
        # output_path = f's3a://pandas-test-eu-west-1/flink/'

        # file_sink = FileSink \
        #     .for_row_format(output_path, Encoder.simple_string_encoder()) \
        #     .with_bucket_assigner(BucketAssigner.date_time_bucket_assigner()) \
        #     .with_output_file_config(OutputFileConfig.builder().with_part_prefix('data').with_part_suffix('.json').build()) \
        #     .with_rolling_policy(RollingPolicy.default_rolling_policy( \
        #         part_size=1024 ** 3, rollover_interval=30 * 1000, inactivity_interval=30 * 1000)) \
        #     .build()

        self.env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").print()

        # .sink_to(file_sink)

        self.env.execute("source")

if __name__ == '__main__':
    SourceData(StreamExecutionEnvironment.get_execution_environment()).get_data()