from preprocessing.utils.dask_cluster import DaskCluster
from preprocessing.utils.sensor import Sensor
from preprocessing.utils.stage_def import FlowStep, FlowStepError


class RestartDaskClusterWorkers(FlowStep):

    @classmethod
    @FlowStep.step(task_run_name='restart_dask_workers_service')
    def run(cls, dask_cluster: DaskCluster):
        response, prev_tasks_arns = dask_cluster.restart_workers_service()
        Sensor.wait_for_service(func=dask_cluster.dask_cluster_workers_service_status_restart,
                                prev_tasks_arns=prev_tasks_arns,
                                )
        return dask_cluster

    @classmethod
    def rollback(cls, exception: FlowStepError):
        print(f"Rolling back {cls.__name__}")

        return super(RestartDaskClusterWorkers, cls).rollback(exception=exception)
