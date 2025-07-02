import logging
import time
from abc import ABCMeta
from datetime import timedelta
from functools import wraps
from typing import Dict, Type

import boto3
import pandas as pd
import dask.dataframe as dd
from dask.dataframe import DataFrame
from prefect import task, flow, get_run_logger, Task
from prefect.context import TaskRun
from prefect.states import State, Completed
from prefect.states import Failed
from cloudwatch import cloudwatch
from prefect.tasks import task_input_hash
from prefect.utilities.hashing import hash_objects

from preprocessing.utils.defaults import AWS_REGION, DATALAKE_BUCKET, DAG_TABLE_ID


def get_storage_options_for_ddf_dask(aws_region):
    session = boto3.Session(region_name=aws_region)
    credentials = session.get_credentials().get_frozen_credentials()

    return {
        'key': credentials.access_key,
        'secret': credentials.secret_key,
        'client_kwargs': {
            'region_name': aws_region
        }
    }


class FlowStepError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class FlowMeta(ABCMeta):
    """Custom metaclass to handle operator overloading for classes"""

    def __rshift__(cls, other):
        """Overload the >> operator to chain classes"""
        cls._next_step = other
        other._previous_step = cls
        return other


class FlowStep(metaclass=FlowMeta):
    _previous_step = None
    _next_step = None


    @staticmethod
    def custom_task_input_hash(context, arguments):
        # Exclude problematic keys like 'flow_meta'
        filtered_args = {k: v for k, v in arguments.items() if k != "flow_meta"}

        try:
            return hash_objects(
                context.task.task_key,
                context.task.fn.__code__.co_code.hex(),
                filtered_args,
            )
        except Exception:
            return None

    @staticmethod
    def step(task_run_name, **kwargs):
        """Combines FlowStep.handle_rollback, task, and classmethod"""

        #
        # cache_expiration = kwargs.pop(  # allow override
        #     "cache_expiration", timedelta(days=7)
        # )
        #
        # def on_completion_hook(task, task_run, state):
        #     logger = get_run_logger()
        #     if state.name == 'Completed':
        #         logger.info(f"Task {task.task_run_name} completed successfully")

        def decorator(func):
            # Apply each decorator manually inside this combined decorator
            if 'retries' in kwargs:
                attempt = 0
                func = FlowStep.handle_rollback(func, kwargs['retries'], attempt=attempt)  # Apply handle_rollback
            else:
                func = FlowStep.handle_rollback(func)
            func = task(
                task_run_name=task_run_name,
                # on_completion=[on_completion_hook],
                cache_key_fn=FlowStep.custom_task_input_hash,
                # cache_expiration=cache_expiration,
                **kwargs)(func)  # Apply task with task_run_name
            return func

        return decorator


    @staticmethod
    def step_flow(flow_run_name):
        """Combines FlowStep.handle_rollback, task, and classmethod"""

        def decorator(func):
            # Apply each decorator manually inside this combined decorator
            func = FlowStep.handle_rollback(func)  # Apply handle_rollback
            func = flow(flow_run_name=flow_run_name)(func)  # Apply task with task_run_name
            return func

        return decorator


    @staticmethod
    def save_result_to_datalake(result_ddf: DataFrame, flow_information: Dict[str, str], stage_type: Type,
                                result_name: str = 'results') -> str:
        s3_path = f's3://{DATALAKE_BUCKET}/stages/${flow_information[DAG_TABLE_ID]}/{stage_type.__name__}/{result_name}.parquet.gzip'
        result_ddf.to_parquet(
            s3_path,
            engine='auto',
            compression='snappy',
            write_index=False,
            storage_options=get_storage_options_for_ddf_dask(AWS_REGION)
        )
        return s3_path


    @staticmethod
    def get_aws_client_for_dask_worker(client_type: str, storage_options: dict):
        return boto3.client(
            client_type,
            aws_access_key_id=storage_options['key'],
            aws_secret_access_key=storage_options['secret'],
            region_name=storage_options['client_kwargs']['region_name']
        )


    @staticmethod
    def read_from_datalake(s3_path_parquet: str, meta: pd.DataFrame):
        ddf_from_parquet = dd.read_parquet(s3_path_parquet,
                                           engine='auto',
                                           storage_options=get_storage_options_for_ddf_dask(AWS_REGION)
                                           )
        ddf_from_parquet = ddf_from_parquet.astype(
            {col: dtype.name for col, dtype in meta.dtypes.to_dict().items()})
        return ddf_from_parquet


    @staticmethod
    def handle_rollback(func, retries=None, attempt=0):
        """Static method decorator to handle exceptions and call rollback"""

        def wrapper(*args, **kwargs):
            nonlocal attempt  # TODO bad practice, but necessary for now, change this later
            try:
                # Try to execute the function
                return func(*args, **kwargs)
            except Exception as e:
                # Handle the exception, print an error message, and call rollback
                logger = get_run_logger()
                if retries and attempt < retries:
                    logger.warning(f"An error occurred in {func.__name__}: {e}. Retrying {retries - attempt} more times.")
                    attempt += 1
                    raise
                else:
                    logger.error(f"All retry attempts exhausted for {func.__name__}. Rolling back...")
                    cls = args[0]
                    cls.rollback(exception=e)

        return wrapper


    @classmethod
    def set_previous_step(cls, step: 'FlowStep'):
        """Set the previous step for the current class."""
        cls._previous_step = step


    @classmethod
    def set_next_step(cls, step: 'FlowStep'):
        """Set the next step for the current class."""
        cls._next_step = step


    @classmethod
    def get_previous_step(cls) -> 'FlowStep':
        """Get the previous step for the current class."""
        return cls._previous_step


    @classmethod
    def get_next_step(cls) -> 'FlowStep':
        """Get the next step for the current class."""
        return cls._next_step


    @classmethod
    def run(cls, *args, **kwargs):
        pass


    @classmethod
    def rollback(cls, exception: FlowStepError):
        if cls.get_previous_step():
            print(f"Parent run method called in {cls.__name__}")
            cls.get_previous_step().rollback(exception)
        else:
            raise exception
