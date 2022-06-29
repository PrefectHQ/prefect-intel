import asyncio, platform
from prefect import flow, task
from prefect.task_runners import DaskTaskRunner, SequentialTaskRunner

@task
def hello_local():
    print("Hello!")

@task
def hello_dask():
    print("Hello from Dask!")

@flow(task_runner=SequentialTaskRunner())
def sequential_flow():
    hello_local()
    dask_subflow()
    hello_local()

@flow(task_runner=DaskTaskRunner())
def dask_subflow():
    hello_dask()

if __name__ == "__main__":
    if platform.system()=='Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sequential_flow()
