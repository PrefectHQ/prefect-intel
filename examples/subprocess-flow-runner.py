from prefect import flow
from datetime import datetime
from prefect.deployments import DeploymentSpec
from prefect.flow_runners import SubprocessFlowRunner

@flow
def what_day_is_it(date: datetime = None):
    if date is None:
        date = datetime.utcnow()
    print(f"It was {date.strftime('%A')} on {date.isoformat()}")

DeploymentSpec(
    flow=what_day_is_it,
    name="flow-deployment",
    # tags=["dev"],
    flow_runner=SubprocessFlowRunner(env={"MY_VARIABLE": "FOO"}, condaenv="prefect-intel")
    # schedule=RRuleSchedule(
    #     rrule="FREQ=MONTHLY;INTERVAL=1;BYDAY=3FR", # 3rd Friday of every month
    #     timezone="America/New_York"
)

if __name__ == "__main__":
    what_day_is_it("2021-01-01T02:00:19.180906")
    # It was Friday on 2021-01-01T02:00:19.180906

