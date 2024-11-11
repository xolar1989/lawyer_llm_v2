from preprocessing.utils.dask_cluster import DaskCluster
from preprocessing.utils.sensor import Sensor
from preprocessing.utils.stage_def import FlowStep, FlowStepError


class UpdateDaskClusterWorkers(FlowStep):
    @classmethod
    @FlowStep.step(task_run_name='update_dask_workers_count')
    def run(cls, dask_cluster: DaskCluster, desired_count: int):
        dask_cluster.update_dask_workers_count(desired_count=desired_count)
        Sensor.wait_for_service(func=dask_cluster.dask_cluster_workers_service_status, desired_count=desired_count)
        return dask_cluster

    @classmethod
    def rollback(cls, exception: FlowStepError):
        print(f"Rolling back {cls.__name__}")

        return super(UpdateDaskClusterWorkers, cls).rollback(exception=exception)