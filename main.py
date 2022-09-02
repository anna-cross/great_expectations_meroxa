import logging
import pandas
from sqlalchemy import true
from turbine.runtime import RecordList, Runtime
from meroxa_expectations import expectations_run
from meroxa_utils import alert
import typing as t
import os
from dotenv import load_dotenv
import pdb
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)
logging.basicConfig(level=logging.info)
import gc

def validate(records: RecordList) -> RecordList:
    logging.info(f"processing {len(records)} record(s)")
    ret = RecordList()
    for record in records:
        try:
            payload = record.value["payload"]

            df = pandas.DataFrame([payload])
            res, validation = expectations_run(df)
            
            if res != True:
                alert(validation)
                print(f"Validation failed for records: \n {payload} \n \n " , flush=True)
                logging.info(f"Validation failed for records: \n {payload} \n \n")
            else: 
                ret.append(record)
                print(f"Success on output: {payload}",flush=true)
            
        except Exception as e:
            print("Error occurred while parsing records: " + str(e), flush=True)
    records = ret
    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:        
            logging.info("Very important info - my tears")


            logging.info("########################### Get Source")

            source = await turbine.resources("source")

            logging.info("########################### Get Records")

            records = await source.records("orders", {})

            logging.info("########################### Run Validate")


            validated = await turbine.process(records, validate)

            logging.info("########################### Get Dest")

            destination_db = await turbine.resources("dest")

            await destination_db.write(validated, "dest_table", {})
        except Exception as e:
            print(e, flush=True)
