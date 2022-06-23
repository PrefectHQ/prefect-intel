import asyncio, random, platform, time
from prefect import task, flow
from prefect.task_runners import DaskTaskRunner

@task
def sleep(secs):
    print('Beginning to sleep...')
    time.sleep(secs)
    # returns a number in range [0.0, 1.0)
    return random.random()

@task
def fail():
    raise TypeError("Something was misconfigured")

@flow(task_runner=DaskTaskRunner())
def complex_flow_logic():
    long_sleep = sleep(10)

    time.sleep(2)
    if long_sleep.get_state().is_running():
        # can run custom code here!
        # including conditionally running other tasks
        print('Long sleep task is still running!')

    # blocks until complete and returns state
    state = long_sleep.wait() 
    print(f"{state=}")
    if state.result(raise_on_failure=False) > 0.5:
        # conditionally run another task based on the output
        print('running fail task')
        fail()
    else:
        print('result was good')

if __name__ == "__main__":
    if platform.system()=='Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    complex_flow_logic()