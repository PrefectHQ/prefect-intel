from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
import random

@task
def stop_at_floor(floor):
    situation = random.choice(["on fire","clear"])
    print(f"elevator stops on {floor} which is {situation}")

@flow(task_runner=SequentialTaskRunner(),
      name="towering-infernflow",
      )
def glass_tower():
    for floor in range(1,39):
        stop_at_floor(floor)

glass_tower()
