import time, random, platform
from prefect import flow, get_run_logger
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import DockerFlowRunner, SubprocessFlowRunner, UniversalFlowRunner

@flow
def hello_world(name: str = "world"):
    logger = get_run_logger()
    logger.info(f"Hello {name}!")

# DeploymentSpec(
#     flow_location="hello-world.py",
#     name="flow-deployment-subprocess", 
#     flow_runner=SubprocessFlowRunner()
# )

DeploymentSpec(
    flow_location="hello-world.py",
    name="flow-deployment", 
) #UniversalFlowRunner

# DeploymentSpec(
#     flow_location="hello-world.py",
#     name="flow-deployment", 
#     flow_runner=UniversalFlowRunner()
# )

# DeploymentSpec(
#     flow_location="hello-world.py",
#     name="flow-deployment-docker", 
#     flow_runner=DockerFlowRunner()
# )


if __name__ == "__main__":
    hello_world()
