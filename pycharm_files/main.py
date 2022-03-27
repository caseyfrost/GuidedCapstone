from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, TimestampType, StructType
from pyspark.sql import SparkSession
import IPython
import json


def common_event(trade_dt: DateType, rec_type: StringType, symbol: StringType, exchange: StringType,
                 event_tm: TimestampType, event_seq_nb: IntegerType, arrival_tm: TimestampType,
                 trade_pr: DecimalType(30, 15), trade_size: IntegerType, bid_pr: DecimalType(30, 15),
                 bid_size: IntegerType, ask_pr: DecimalType(30, 15), ask_size: IntegerType, partition: StringType,
                 line: StringType):
    """Returns common event schema

    Union of fields for both quotes and trades within the json and csv source files. Neither quotes nor trades have
    all of these fields, so some values will be None.

    Args:
        trade_dt: date of trade, date type
        rec_type: record type T or Q, string type
        symbol: stock symbol, string type
        exchange: stock exchange, string type
        event_tm: event time, timestamp type
        event_seq_nb: sequence number in .txt file, integer type
        arrival_tm: arrival time, timestamp type
        trade_pr: trade price, decimal type
        trade_size: size of trade, integer type
        bid_pr: bid price, decimal type
        bid_size: size of bid, integer type
        ask_pr: ask price, decimal type
        ask_size: ask size, decimal type
        partition: partition key for trade quote or bad T,Q, or B
        line: used to return bad line
    Returns:
        either the bad line or good record as list of values for each field"""

    if partition == "B":
        return line
    else:
        return [trade_dt, rec_type, symbol, exchange,
                event_tm, event_seq_nb, arrival_tm,
                trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size, partition]


# noinspection PyTypeChecker
def parse_csv(line: str):
    """CSV parser to be used in Spark transformation process"""
    record_type_pos = 2
    record = line.split(',')
    try:
        # logic to parse records
        if record[record_type_pos] == 'T':
            event = common_event(datetime.strptime(record[0], "%Y-%m-%d"), record[2], record[3], record[6],
                                 datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'), int(record[5]),
                                 datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'), Decimal(record[7]),
                                 int(record[8]), None, None, None, None, 'T', '')
            return event
        elif record[record_type_pos] == 'Q':
            event = common_event(datetime.strptime(record[0], "%Y-%m-%d"), record[2], record[3], record[6],
                                 datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'), int(record[5]),
                                 datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'), None, None, Decimal(record[7]),
                                 int(record[8]), Decimal(record[9]), int(record[10]), 'Q', '')
            return event
    except Exception as e:
        # save record to dummy event in bad partition
        # fill in the fields as None or empty string
        print(e)
        return common_event(None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line)


def process_csv(csv_name):
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    spark.conf.set('fs.azure.account.key.gcblobcf2022.blob.core.windows.net',
                   'h3lPjueXOs8RE37xrWUD70ZNsHp4wUo/BeGWX6FlLieE/8RZlRWn1VGMT/QoqcF7h2KwXS34RoyVYoUIqS7MeA==')
    # raw = spark.sparkContext.textFile(f'wasbs://testcontainer@gcblobcf2022.blob.core.windows.net/{csv_name}')
    raw = spark.sparkContext.textFile(f'wasbs://testcontainer@gcblobcf2022.blob.core.windows.net/{csv_name}')
    parsed = raw.map(lambda line: parse_csv(line))
    data = spark.createDataFrame(parsed)
    return data


# noinspection PyTypeChecker
def parse_json(line: str):
    record = json.loads(line)
    record_type = record['event_type']
    try:
        # logic to parse records
        if record_type == "T":
            event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"],
                                 record["symbol"], record["exchange"],
                                 datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'),
                                 int(record["event_seq_nb"]),
                                 datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), Decimal(record["price"]),
                                 int(record["size"]), None, None, None, None, "T", None)
            return event
        elif record_type == 'Q':
            event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"],
                                 record["symbol"], record["exchange"],
                                 datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'),
                                 int(record["event_seq_nb"]),
                                 datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), None, None,
                                 Decimal(record["bid_pr"]), int(record["bid_size"]), Decimal(record["ask_pr"]),
                                 int(record["ask_size"]), "Q", None)
            return event
    except Exception as e:
        print(e)
        return common_event(None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line)


def process_json(json_name):
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    raw = spark.sparkContext.textFile(f'wasbs://testcontainer@gcblobcf2022.blob.core.windows.net/{json_name}')
    parsed = raw.map(lambda line: parse_csv(line))
    data = spark.createDataFrame(parsed)
    return data


schema = StructType().add("trade_dt", DateType()).add("rec_type", StringType()).add("symbol", StringType())\
    .add("exchange", StringType()).add("event_tm", TimestampType()).add("event_seq_nb", IntegerType())\
    .add("arrival_tm", TimestampType()).add("trade_pr", DecimalType()).add("trade_size", IntegerType())\
    .add("bid_pr", DecimalType()).add("bid_size", IntegerType()).add("ask_pr", DecimalType())\
    .add("ask_size", IntegerType()).add("partition", StringType()).add("line", StringType())
