import inspect
import logging
import time
import uuid
from functools import wraps
from types import FunctionType
from typing import Callable

import boto3
import pandas as pd
from botocore.client import BaseClient
from cloudwatch import cloudwatch
from dask import delayed
from dask.distributed import Client, as_completed
from botocore.exceptions import ClientError
import dask.dataframe as dd
from tqdm import tqdm

from preprocessing.dask.dask_cluster import DaskCluster
from preprocessing.utils.defaults import AWS_REGION, DAG_TABLE_ID
from preprocessing.utils.general import timeit
from preprocessing.utils.sensor import Sensor
from preprocessing.utils.stage_def import get_storage_options_for_ddf_dask


class FargateDaskCluster(DaskCluster):

    def __init__(self, stack_name: str, cluster_name: str, workers_service_name: str,
                 cloudformation_client: BaseClient, ecs_client: BaseClient):
        self.stack_name = stack_name
        self.cluster_name = cluster_name
        self.workers_service_name = workers_service_name
        self.cloudformation_client = cloudformation_client
        self.ecs_client = ecs_client

    def get_cluster_url(self) -> str:
        return self.get_stack_output_value("DaskSchedulerALBURL", self.stack_name, self.cloudformation_client)

    @classmethod
    def build(cls, stack_name: str, cluster_name: str, workers_service_name: str, flow_run_id: uuid,
              flow_run_name: str, cluster_props: dict = None):
        cloudformation_client = boto3.client('cloudformation', region_name=AWS_REGION)
        ecs_client = boto3.client('ecs', region_name=AWS_REGION)
        stack_id = cls.create_dask_cluster(
            cluster_name=cluster_name,
            stack_name=stack_name,
            workers_service=workers_service_name,
            flow_run_id=flow_run_id,
            flow_run_name=flow_run_name,
            cloudformation_client=cloudformation_client,
            cluster_props=cluster_props
        )
        if stack_id:
            Sensor.wait_for_service(func=cls.dask_cluster_action_status,
                                    stack_name=stack_name,
                                    cloudformation_client=cloudformation_client,
                                    action_type='CREATE'
                                    )
        return cls(
            stack_name=stack_name,
            cluster_name=cluster_name,
            workers_service_name=workers_service_name,
            cloudformation_client=cloudformation_client,
            ecs_client=ecs_client
        )

    @classmethod
    def from_existing(cls, stack_name: str):
        cloudformation_client = boto3.client('cloudformation', region_name=AWS_REGION)
        ecs_client = boto3.client('ecs', region_name=AWS_REGION)
        cluster_name = cls.get_stack_output_value("DaskCluster", stack_name, cloudformation_client)
        workers_service_name = cls.get_stack_output_value("WorkersServiceName", stack_name, cloudformation_client)
        return cls(
            stack_name=stack_name,
            cluster_name=cluster_name,
            workers_service_name=workers_service_name,
            cloudformation_client=cloudformation_client,
            ecs_client=ecs_client
        )

    @staticmethod
    def create_dask_cluster(stack_name: str, cluster_name: str, workers_service: str, flow_run_id: uuid,
                            flow_run_name: str, cloudformation_client: BaseClient, cluster_props: dict):
        template_body = open('dask-iaas.yaml', 'r').read()
        parameters = [
            {
                'ParameterKey': 'ECSClusterName',
                'ParameterValue': cluster_name
            },
            {
                'ParameterKey': 'WorkersServiceName',
                'ParameterValue': workers_service
            },
            {
                'ParameterKey': 'FlowRunId',
                'ParameterValue': str(flow_run_id)
            },
            {
                'ParameterKey': 'FlowRunName',
                'ParameterValue': flow_run_name
            }
        ]
        if cluster_props:
            parameters.extend(
                {
                    'ParameterKey': key,
                    'ParameterValue': str(value)
                } for key, value in cluster_props.items()
            )
        try:
            response = cloudformation_client.create_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=parameters,
                Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM'],
                OnFailure='ROLLBACK'
            )
            print(f"Stack creation initiated: {response['StackId']}")
            return response['StackId']
        except Exception as e:
            print(f"An error occurred: {e}")
            raise Exception(f"An error occurred: {e}")

    @staticmethod
    def dask_cluster_action_status(stack_name: str, cloudformation_client: BaseClient, action_type: str = "CREATE"):
        try:
            response = cloudformation_client.describe_stacks(StackName=stack_name)
            stack_status = response['Stacks'][0]['StackStatus']
            print(f"Current stack status: {stack_status}")

            if stack_status in ['CREATE_FAILED', 'UPDATE_FAILED', 'ROLLBACK_FAILED', 'ROLLBACK_COMPLETE']:
                raise Exception(f"Stack {stack_name} is in a failed state {stack_status} in action: {action_type}")

            return stack_status == 'DELETE_COMPLETE' if action_type == 'DELETE' else stack_status == 'CREATE_COMPLETE'
        except ClientError as e:
            # If the stack doesn't exist anymore, it means the deletion was successful
            if 'does not exist' in str(e):
                print(f"Stack {stack_name} no longer exists. {action_type} complete.")
                return True
            else:
                print(f"An error occurred: {e}")
                raise

    @staticmethod
    def get_stack_output_value(output_key, stack_name: str, cloudformation_client: BaseClient):
        response = cloudformation_client.describe_stacks(StackName=stack_name)

        outputs = response['Stacks'][0].get('Outputs', [])

        # Look for the specific output key
        for output in outputs:
            if output['OutputKey'] == output_key:
                return output['OutputValue']

        # Raise an exception if the key is not found
        raise Exception(f"Output key '{output_key}' not found in the stack '{stack_name}'")

    def update_dask_workers_count(self, desired_count: int):
        try:
            response = self.ecs_client.update_service(
                cluster=self.cluster_name,
                service=self.workers_service_name,
                desiredCount=desired_count
            )
            return response
        except Exception as e:
            raise Exception(f"Error updating workers count: {str(e)}")

    # def restart_workers_service(self):
    #     def get_env_var_exists(name, container_def):
    #         for env_var in container_def['environment']:
    #             if env_var['name'] == name:
    #                 return True
    #         return False
    #
    #     def set_env_var_value(name, value, container_def):
    #         for env_var in container_def['environment']:
    #             if env_var['name'] == name:
    #                 env_var['value'] = value
    #
    #     deploy_version = f"v{int(time.time())}"
    #     try:
    #         worker_task_definition = self.get_stack_output_value("TaskDefinitionWorkerName",
    #                                                              self.stack_name, self.cloudformation_client)
    #         task_definition = self.ecs_client.describe_task_definition(taskDefinition=worker_task_definition)
    #
    #         new_container_definitions = task_definition['taskDefinition']['containerDefinitions']
    #         for container_def in new_container_definitions:
    #             if get_env_var_exists('DEPLOY_VERSION', container_def):
    #                 set_env_var_value('DEPLOY_VERSION', deploy_version, container_def)
    #             else:
    #                 container_def['environment'].append({'name': 'DEPLOY_VERSION', 'value': deploy_version})
    #
    #         response = self.ecs_client.register_task_definition(
    #             family=task_definition['taskDefinition']['family'],
    #             taskRoleArn=task_definition['taskDefinition']['taskRoleArn'],
    #             executionRoleArn=task_definition['taskDefinition']['executionRoleArn'],
    #             networkMode=task_definition['taskDefinition']['networkMode'],
    #             containerDefinitions=new_container_definitions,
    #             requiresCompatibilities=task_definition['taskDefinition']['requiresCompatibilities'],
    #             cpu=task_definition['taskDefinition']['cpu'],
    #             memory=task_definition['taskDefinition']['memory'],
    #         )
    #
    #         response = self.ecs_client.update_service(
    #             cluster=self.cluster_name,
    #             service=self.workers_service_name,
    #             taskDefinition=response['taskDefinition']['taskDefinitionArn'],
    #             forceNewDeployment=True,
    #             propagateTags='TASK_DEFINITION'
    #         )
    #         return response, deploy_version
    #     except Exception as e:
    #         raise Exception(f"Error restarting workers service: {str(e)}")

    def restart_workers_service(self):

        ## Todo get here all arns of task and in dask_cluster_workers_service_status_restart check that certain arn is not in this list previous tasks arns
        task_arns = self.ecs_client.list_tasks(
            cluster=self.cluster_name,
            serviceName=self.workers_service_name,
            desiredStatus='RUNNING'
        )['taskArns']

        try:

            response = self.ecs_client.update_service(
                cluster=self.cluster_name,
                service=self.workers_service_name,
                forceNewDeployment=True
            )
            return response, task_arns
        except Exception as e:
            raise Exception(f"Error restarting workers service: {str(e)}")

    def get_workers_count(self):
        try:
            response = self.ecs_client.describe_services(
                cluster=self.cluster_name, services=[self.workers_service_name]
            )

            # Check if there are any failures in the response
            if 'failures' in response and len(response['failures']) > 0:
                raise Exception(f"Failed to describe service {self.workers_service_name}: {response['failures']}")

            # Ensure the response contains valid services data
            if 'services' not in response or len(response['services']) == 0:
                raise Exception(f"No services found for {self.workers_service_name} in the cluster {self.cluster_name}")

            # Retrieve and return the running worker count
            workers_count = response['services'][0]['runningCount']
            return workers_count

        except Exception as e:
            raise Exception(f"Error retrieving workers count: {str(e)}")

    def dask_cluster_workers_service_status(self, desired_count: int):
        response = self.ecs_client.describe_services(cluster=self.cluster_name, services=[self.workers_service_name])
        if 'failures' in response and len(response['failures']) > 0:
            raise Exception(f"Failed to describe service {self.workers_service_name}: {response['failures']}")
        running_count = response['services'][0]['runningCount']
        print(f"Currently running tasks: {running_count}")

        # Return True if the desired count matches the running count
        return running_count == desired_count

    def have_container_this_version(self, container_def, deploy_version):
        for env_var in container_def.get('environment', []):
            if env_var['name'] == 'DEPLOY_VERSION' and env_var['value'] == deploy_version:
                return True
        return False

    def dask_cluster_workers_service_status_restart(self, prev_tasks_arns: str):
        response = self.ecs_client.list_tasks(
            cluster=self.cluster_name,
            serviceName=self.workers_service_name,
            desiredStatus='RUNNING'
        )

        current_task_arns = response['taskArns']
        if not current_task_arns:
            raise Exception(f"No tasks found for service {self.workers_service_name}")

        new_restart_tasks_arns = list(set(current_task_arns) - set(prev_tasks_arns))

        new_running_tasks = []
        if new_restart_tasks_arns:
            tasks_description = self.ecs_client.describe_tasks(
                cluster=self.cluster_name,
                tasks=new_restart_tasks_arns
            )

            # Filter tasks that are actually running
            new_running_tasks = [
                task['taskArn'] for task in tasks_description['tasks'] if task['lastStatus'] == 'RUNNING'
            ]
            r = 4

        print(f"Amount of tasks before restart: {len(prev_tasks_arns)}, new tasks have already launched: "
              f"{len(new_running_tasks)}")

        # Return True if the desired count of restarted tasks matches the running count
        return len(new_running_tasks) == len(prev_tasks_arns)

    @timeit
    def delete_dask_cluster(self):
        try:
            try:
                self.cloudformation_client.describe_stacks(StackName=self.stack_name)
            except self.cloudformation_client.exceptions.ClientError as e:
                if "does not exist" in str(e):
                    print(f"Stack {self.stack_name} already deleted.")
                    return
                raise


            response = self.cloudformation_client.delete_stack(StackName=self.stack_name)
            print(f"Deleting stack {self.stack_name}...")
            waiter = self.cloudformation_client.get_waiter('stack_delete_complete')
            waiter.wait(StackName=self.stack_name)
            return response
        except Exception as e:
            print(f"An error occurred: {e}")
            raise Exception(f"An error occurred: {e}")



def time_execution_dask_task(func):
    aws_info = get_storage_options_for_ddf_dask(AWS_REGION)

    """Decorator to measure the execution time of a function and log flow information."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        flow_information = kwargs.get('flow_information')
        if not flow_information:
            raise ValueError("Flow information not provided.")
        logger = logging.getLogger(f"{func.__name__}/{flow_information[DAG_TABLE_ID]}")
        # Create the formatter
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        # ---- Create the Cloudwatch Handler ----
        handler = cloudwatch.CloudwatchHandler(
            access_id=aws_info['key'],
            access_key=aws_info['secret'],
            region=aws_info['client_kwargs']['region_name'],
            log_group=f'preprocessing/{func.__name__}',
            log_stream=f'{flow_information[DAG_TABLE_ID]}'
        )
        handler.setFormatter(formatter)
        logger.setLevel(logging.WARNING)
        logger.addHandler(handler)

        start_time = time.time()

        # Run the actual function
        result = func(*args, **kwargs)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(
            f"Execution time for {func.__name__}: {elapsed_time:.2f} seconds with flow_information: {flow_information}")

        return result

    return wrapper


def retry_dask_task(retries: int = 3, delay: int = 5):
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    def decorator(func):

        sig = inspect.signature(func)
        accepts_retry = "retry_attempt" in sig.parameters
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    if accepts_retry:
                        kwargs["retry_attempt"] = attempt
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    func_name = func.__name__
                    logger.warning(
                        f"Function '{func_name}' - Attempt {attempt} failed: {str(e)}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    if attempt == retries:
                        raise  # Rethrow the exception after the last attempt

        return wrapper

    return decorator
