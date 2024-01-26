import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://Users/daniel.ford/Documents/GitHub/danielfordfc/python-playground/flink/flink-1.17.2/lib/flink-connector-kafka-1.17.2.jar,file://Users/daniel.ford/Documents/GitHub/danielfordfc/python-playground/flink/flink-1.17.2/lib/flink-sql-connector-kafka-1.17.2.jar")


    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE flink_test_1_table (
            my_int BIGINT,
            my_str VARCHAR,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flink-test-1',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'table-api-group',
            'format' = 'json'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('flink_test_1_table')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################
    sql = """
        SELECT
          my_str,
          TUMBLE_END(proctime, INTERVAL '60' SECONDS) AS window_end,
          SUM(my_int) * 0.85 AS window_sales
        FROM flink_test_1_table
        GROUP BY
          TUMBLE(proctime, INTERVAL '60' SECONDS),
          my_str
    """
    revenue_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    revenue_tbl.print_schema()

    ###############################################################
    # Create Kafka Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE sales_euros (
            my_str VARCHAR,
            window_end TIMESTAMP(3),
            window_sales DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-euros',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """

    # I manually created sales_euros topic in kafka
    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    revenue_tbl.execute_insert('sales_euros').wait()

    tbl_env.execute('windowed-sales-euros')


if __name__ == '__main__':
    main()