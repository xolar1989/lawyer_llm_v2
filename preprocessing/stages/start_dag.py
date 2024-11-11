import uuid
from datetime import datetime

import boto3

from preprocessing.utils.defaults import DAG_TABLE_ID, AWS_REGION, DAG_DYNAMODB_TABLE_NAME
from preprocessing.utils.dynamodb_helper import put_item_into_table, update_item
from preprocessing.utils.stage_def import FlowStep, FlowStepError


class StartDag(FlowStep):
    dag_information = None

    @classmethod
    @FlowStep.step(task_run_name='start_dag',  retries=3, retry_delay_seconds=10)
    def run(cls, flow_run_id: uuid, flow_run_name: str):
        start_dag_item = {
            DAG_TABLE_ID: str(flow_run_id),
            "flow_run_name": flow_run_name,
            "date_of_creation": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "dag_status": 'IN_PROGRESS'  # IN_PROGRESS, SUCCESS, FAILED,
        }
        boto_session = boto3.Session(region_name=AWS_REGION)
        item = put_item_into_table(DAG_DYNAMODB_TABLE_NAME, start_dag_item, boto_session)
        # cls.dag_information = item
        return item

    @classmethod
    def rollback(cls, exception: FlowStepError):
        print(f"Rolling back {cls.__name__}")
        if cls.dag_information:
            print(f"Updating DAG status to FAILED")
            updated_item = update_item(DAG_DYNAMODB_TABLE_NAME, {DAG_TABLE_ID: cls.dag_information[DAG_TABLE_ID]},
                                       {"dag_status": "FAILED",
                                        "date_of_completion": datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                                       boto3.Session(region_name=AWS_REGION))
            print(updated_item)

        return super(StartDag, cls).rollback(exception=exception)