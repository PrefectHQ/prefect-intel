from prefect import flow
from datetime import datetime
from prefect.blocks.kubernetes import KubernetesCluster
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import KubernetesFlowRunner

@flow
def what_day_is_it(date: datetime = None):
    if date is None:
        date = datetime.utcnow()
    print(f"It was {date.strftime('%A')} on {date.isoformat()}")

DeploymentSpec(
    flow=what_day_is_it,
    name="flow-deployment",
    flow_runner=KubernetesFlowRunner(
        cluster_config=KubernetesCluster()
    )
)

if __name__ == "__main__":
    what_day_is_it("2021-01-01T02:00:19.180906")
