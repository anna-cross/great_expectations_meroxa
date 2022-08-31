import logging
import pandas
from turbine.runtime import Record, Runtime
from meroxa_expectations import expectations_run
from meroxa_utils import alert
import typing as t
import os
def validate(records: t.List[Record]) -> t.List[Record]:
    logging.info(f"processing {len(records)} record(s)")
    print(f"processing {len(records)} record(s)",  flush=True)

    for record in records:
        logging.info(f"input: {record}")
        try:
            payload = record.value["payload"]
            df = pandas.DataFrame([payload])
            res, validation = expectations_run(df)

            if res != "success":
                records.remove(record)
                alert(validation)
                print(f"Validation failed for records: {record}" , flush=True)
                logging.info(f"Validation failed for records: {record}")
            logging.info(f"output: {record}")
        except Exception as e:
            print("Error occurred while parsing records: " + str(e), flush=True)
            logging.info(f"output: {record}")
    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        logging.basicConfig(level=logging.INFO)
        try:        
            logging.info("Very important info - my tears")

            source = await turbine.resources("source")

            records = await source.records("orders", {})

            validated = await turbine.process(records, validate)

            destination_db = await turbine.resources("dest")

            await destination_db.write(validated, "dest_table", {})
        except Exception as e:
            print(e, flush=True)
