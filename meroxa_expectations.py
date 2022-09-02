from __future__ import annotations
import pandas as pd
import os
import sys
from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, DatabaseStoreBackendDefaults
from great_expectations.checkpoint import Checkpoint

from sqlalchemy import true
from sqlalchemy.engine import make_url, URL
import uuid
import pdb
import gc

def expectations_run(df):
    print(" @@@@@@@@@  START VALIDATION  @@@@@@@@@")

    url = os.environ.get("URL")

    r = make_url(url)
    
    data_context_config = DataContextConfig(
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_store",
        checkpoint_store_name="checkpoint_store",
        stores={
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": {
                        "drivername": r.drivername,
                        "host": r.host,
                        "port": r.port,
                        "username": r.username,
                        "password": r.password,
                        "database": r.database,
                    },
                },
            },
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": {
                        "drivername": r.drivername,
                        "host": r.host,
                        "port": r.port,
                        "username": r.username,
                        "password": r.password,
                        "database": r.database,
                    },
                },
            },
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": {
                        "drivername": r.drivername,
                        "host": r.host,
                        "port": r.port,
                        "username": r.username,
                        "password": r.password,
                        "database": r.database,
                    },
                    "table_name": "ex_test_check",
                    "key_columns": ["expectation_suite_name"]
                },
            },
            "evaluation_store":{
                "class_name": "EvaluationParameterStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": {
                        "drivername": r.drivername,
                        "host": r.host,
                        "port": r.port,
                        "username": r.username,
                        "password": r.password,
                        "database": r.database,
                    },
                    "table_name": "ex_test_eval",
                    "key_columns": ["expectation_suite_name"]
                },
            }
        },

    )
    context = BaseDataContext(project_config=data_context_config)

    batch_id = str(uuid.uuid4())

    datasource_config = {
        "name": "datasource",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": [batch_id],
            },
        },
    }

    print('Set batch')

    context.add_datasource(**datasource_config)
    

    batch_request = RuntimeBatchRequest(
        datasource_name="datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="turbine_datasource",
        runtime_parameters={"batch_data": df}, 
        batch_identifiers={batch_id: batch_id},
        
    )


    context.create_expectation_suite("meroxa", overwrite_existing=True)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="meroxa",
        batch_identifiers={batch_id: batch_id},
    )

    print('Run validations')
    

    validator.expect_column_values_to_match_regex(
        column="email",
        regex=r"^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$"
    )


    validator.expect_column_values_to_be_between(
        column="product_price",
        min_value=1, max_value=1000
    )

    validator.expect_column_values_to_be_in_set(
        column="category",
        value_set=["clothing", "food"],
    )

    validator.save_expectation_suite(
        discard_failed_expectations=False
    )

    print('Save suite.')


    python_config = {
        "name": "in_memory_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
    }


    my_checkpoint = Checkpoint(data_context=context, **python_config)

    
    results = my_checkpoint.run(
        validations=[
            {
                "batch_request": {
                    "datasource_name": "datasource",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": "turbine_datasource",
                    "batch_identifiers": {
                        batch_id: batch_id
                    }
                },

                "expectation_suite_name": "meroxa",
            }
        ],
        batch_request={
            "runtime_parameters": {
                        "batch_data": df
            },
        },
    ) 
   

    print(" *********************************** PROCESSED - {}".format(df),flush=true)

    validation_result = list(results.run_results.items())[0][1]["validation_result"]

    print("Validation ended with {res}".format(res=results['success']), flush=true)
    
    return results["success"], validation_result



    #return True, ""
    

