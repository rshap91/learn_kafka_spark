import io
import logging
import yaml
from fastavro import reader, parse_schema
from pyspark.sql import SparkSession, functions as fn
from pyspark.sql.types import (
    DataType, BooleanType, IntegerType, LongType, StringType, DoubleType, ArrayType, StructType, StructField, TimestampType
)

class Processor:

    def __init__(self, conn_info, avro_schema, dest_table):
        logging.info("Initializing Processor")
        self.conn = None
        self.byte_data = None
        self.table_data = None
        self.conn_info = conn_info
        self.avro_schema = avro_schema
        self.dest_table = dest_table
        

    def open(self, partition_id, epoch_id):        
        logging.info("Opening for partition: %s, epoch: %s", partition_id, epoch_id)

        # initialize byte io to store records in batch
        self.byte_data = io.BytesIO()

        # connect to db
        conn = 'connection'
        self.conn = conn
        return True

    def process(self, row):
        # just write the value to byte_data. Will parse and load in close()
        self.byte_data.write(row.value + b'\n')


    def close(self, error):
        logging.info("Closing! Parsing stream data")
        try:
            if error:
                logging.error(error)
                return

            # deserialize avro
            schema = parse_schema(self.avro_schema)
            columns = [field["name"].lower() for field in self.avro_schema["fields"]]
            rdr = reader(self.byte_data, reader_schema=schema)

            # convert to table records
            for record in rdr:
                row = []
                for col in columns:
                    row.append(record[col])
                self.table_data.append(row)
                

            logging.info("loading data with %s records", len(self.table_data))
            # load table data to db
            col_str = '"' + '", "'.join(columns) + '"'
            bind_row = "(" + ", ".join(["%s" for _ in columns]) + ")"
            bind_str = ",\n".join([bind_row for _ in self.table_data])
            binds = [val for row in self.table_data for val in row]
            stmt = f"""
            insert into {self.dest_table}, ({col_str}) values 
                {bind_str}
            """
        
            with self.conn.cursor() as cur:
                cur.execute(stmt, binds)

        finally:
            # close db
            self.conn.close()


def main():
    spark = SparkSession.builder.appName('consumeEvents').getOrCreate()

    with open("config.yaml") as f:
        config = yaml.load(f, loader=yaml.FullLoader)


    eventDf = (
        spark.readStream.format("kafka")
                        .option("kafka.bootstrap.servers", ",".join(config['kafka_bootstrap_servers'])) # required to connect to kafka
                        .option("subscribe", config['kafkaTopic'])
                        .load()
    )

    q = eventDf.writeStream.foreach(Processor(config["db_conn_info"], config['avro_schema'], config["dest_table"])).start()
    q.awaitTermination()



