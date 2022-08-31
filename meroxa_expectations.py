from __future__ import annotations
import pandas as pd
import os
from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import  RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
import subprocess
import pdb
import stat
import tempfile

def expectations_run(df):

    root_directory = os.path.join(os.path.dirname(__file__), "great_expectations")

    data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
        ),
    )
    context = BaseDataContext(project_config=data_context_config)

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
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))
    
    print('Set batch')

    context.add_datasource(**datasource_config)

    batch_request = RuntimeBatchRequest(
        datasource_name="datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="turbine_datasource",  # This can be anything that identifies this data_asset for you
        runtime_parameters={"batch_data": df},  # df is your dataframe
        batch_identifiers={"default_identifier_name": "default_identifier_name"},
    )

    context.create_expectation_suite("meroxa", overwrite_existing=True)

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="meroxa"
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

    checkpoint_config = {
        "name": "first_check",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "datasource",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": "turbine_datasource",
                },
                "expectation_suite_name": "meroxa",
            }
        ],
    }
    context.add_checkpoint(**checkpoint_config)

    results = context.run_checkpoint(
        checkpoint_name="first_check",
        batch_request={
            "runtime_parameters": {"batch_data": df},
            "batch_identifiers": {
                "default_identifier_name": "default_identifier_name"
            },
        },
    )
    """
     pdb.set_trace()

    results = context.run_checkpoint(
    checkpoint_name="first_check",
    validations=[
            {"batch_request": batch_request},
        ],
    )
    """

    validation_result = list(results.run_results.items())[0][1]["validation_result"]

    return results["success"], validation_result
