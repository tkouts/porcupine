from porcupine.schema import Container, Item, Composite
from porcupine.datatypes import String, DateTime, Float, Embedded


class CronExecution(Composite):
    name = String(required=True)
    started = DateTime(required=True)


class CronStatus(Item):
    status = String(required=True)
    spec = String(required=True)
    last_successful_run = DateTime()
    last_run_status = String()
    execution_time = Float()
    running = Embedded(
        accepts=(CronExecution, ),
        swappable=False
    )


class CronJobs(Container):
    containment = CronStatus,
