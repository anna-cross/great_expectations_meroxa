from __future__ import annotations
import pandas as pd
import os
from typing import Any
from dotenv import load_dotenv
from sqlalchemy import true
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)
from slack_sdk.webhook import WebhookClient
import pdb


def alert(validation):
    webhook = WebhookClient(url=os.environ.get("WEBHOOK_URL"))
    print("-----------------------------------------------")
    print(validation, flush=true)

    validation_err = check_bad_validations(validation)
    block_payload = [
        {
            "type": "image",
            "image_url": "https://avatars.githubusercontent.com/u/31670619?s=200&v=4",
            "alt_text": "inspiration",
        },
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Error on Record Validation",
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "*Evaluated Expectations:*\n {ee}".format(
                        ee=validation["statistics"]["evaluated_expectations"]
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": "*Successful Expectationsype:*\n {se}".format(
                        se=validation["statistics"]["successful_expectations"]
                    ),
                },
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "*Unsuccessful Expectations:*\n {ue}".format(
                        ue=validation["statistics"]["unsuccessful_expectations"]
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": "*Success Percent:*\n {sp}".format(
                        sp=validation["statistics"]["success_percent"]
                    ),
                },
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "{ve}".format(ve=validation_err),
            },
        },
    ]

    webhook.send(blocks=block_payload)


def check_bad_validations(validation):
    err = ""
    for res in validation["results"]:
        if not bool(res["success"]):
            err += "\n Error value on - {value} \n Column - {column} \n Expectation - {expectation} \n ".format(
                value=' '.join(res["result"]["partial_unexpected_list"]),
                expectation=res["expectation_config"]["expectation_type"],
                column=res["expectation_config"]["kwargs"]["column"],
            )
    return err
