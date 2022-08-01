from prefect import flow, task

@task
def my_favorite_task():
    return 42

@flow(name= "Testing favorite flow")
def my_favorite_flow():
    val = my_favorite_task()
    return val

def test_my_favorite_task():
    assert my_favorite_task.fn() == 42
