from prefect import flow, get_run_logger


@flow
def hello_world(name: str = "world"):
    logger = get_run_logger()
    logger.info(f"Hello {name}!")


if __name__ == "__main__":
    hello_world()
