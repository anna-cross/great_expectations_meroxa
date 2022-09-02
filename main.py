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
import gc

def validate(records: RecordList) -> RecordList:
    print(f"processing {len(records)} record(s)",  flush=True)
    ret = RecordList()
    for record in records:
        try:
            payload = record.value["payload"]

            df = pandas.DataFrame([payload])
            res, validation = expectations_run(df)
            
            if res != True:
                alert(validation)
                print(f"Validation failed for records: \n {payload} \n \n " , flush=True)
            else: 
                ret.append(record)
                print(f"Success on output: {payload}",flush=true)
            
        except Exception as e:
            print("Error occurred while parsing records: " + str(e), flush=True)

    print("--- PRINT RET AKA RECORD WE WANT TO SEND TO DEST")
    print(ret)
    records = ret

    print("---- PRINT FINAL RECORDS LIST ---- ")
    print(records)


    print(" ---------- End VALIDATE ---- ")

    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:        
            source = await turbine.resources("source")

            records = await source.records("orders", {})

            validated = await turbine.process(records, validate)
            
            destination_db = await turbine.resources("dest")

            await destination_db.write(validated, "dest_table", {})
            
        except Exception as e:
            print(e, flush=True)
