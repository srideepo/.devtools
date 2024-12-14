import pytest
import os
from app.model import TaskStatus
from app.exceptions import TaskNotFoundError
from app.logger import create_logger

# Extract the filename without extension
filename = os.path.splitext(os.path.basename(__file__))[0]
logger = create_logger(logger_name=filename)

#@pytest.mark.skip()
def test_create_and_read_task(db_instance_empty_mod, db_instance_current_mod, session, task1):
    """
    Test the creation and reading of a task
    """
    # Write Task to DB
    db_instance_empty_mod.create_task(task=task1, session=session)

    # Read Task from DB
    task = db_instance_current_mod.read_task(task_id=1, session=session)
    assert task.title == task1.title
    assert task.description == task1.description
    assert task.status == task1.status

#@pytest.mark.skip()
def test_read_all_tasks(db_instance_empty_fn, session, task1, task2):
    """
    Test the reading of all tasks
    """
    # Write 2 Tasks to DB
    db_instance_empty_fn.create_task(task=task1, session=session)
    db_instance_empty_fn.create_task(task=task2, session=session)

    # Read all Tasks from DB
    tasks = db_instance_empty_fn.read_tasks(session=session)
    assert len(tasks) == 2
    assert tasks[0].title == task1.title
    assert tasks[1].title == task2.title

#@pytest.mark.skip()
def test_read_all_tasks_empty(db_instance_empty_fn, session):
    """
    Test the reading of all tasks when the DB is empty
    """
    # Read all Tasks from DB
    tasks = db_instance_empty_fn.read_tasks(session=session)
    assert len(tasks) == 0

#@pytest.mark.skip()
def test_delete_task(db_instance_empty_fn, session, task1, task2):
    """
    Test the deletion of a task
    """
    # Write 2 Tasks to DB
    db_instance_empty_fn.create_task(task=task1, session=session)
    db_instance_empty_fn.create_task(task=task2, session=session)

    # Delete Task
    db_instance_empty_fn.delete_task(session=session, task_id=1)

    # Read Task from DB
    with pytest.raises(TaskNotFoundError):
        db_instance_empty_fn.read_task(task_id=1, session=session)

#@pytest.mark.skip()
def test_delete_all_tasks(db_instance_empty_fn, session, task1, task2):
    """
    Test the deletion of all tasks
    """
    # Write 2 Tasks to DB
    db_instance_empty_fn.create_task(task=task1, session=session)
    db_instance_empty_fn.create_task(task=task2, session=session)

    # Delete all Tasks from DB
    db_instance_empty_fn.delete_all_tasks(session=session)

    # Read all Tasks from DB
    tasks = db_instance_empty_fn.read_tasks(session=session)
    assert len(tasks) == 0

#@pytest.mark.skip()
def test_update_task(db_instance_empty_fn, session, task1):
    """
    Test the updating of a task (status)
    """
    # Write Task to DB
    db_instance_empty_fn.create_task(task=task1, session=session)

    # Update Task
    db_instance_empty_fn.update_task(
        session=session,
        task_id=1,
        task_status=TaskStatus.COMPLETED,
        task_title="Wash The Car",  # Change from "Go to the Gym" to "Wash The Car"
    )

    # Read Task from DB
    task = db_instance_empty_fn.read_task(task_id=1, session=session)

    # Check Task Status and Updated At
    assert task.status == TaskStatus.COMPLETED
    assert task.title == "Wash The Car"
    assert task.updated_at > task.created_at

    # Check that the description has not changed
    assert task.description == task1.description

#@pytest.mark.skip()
def test_update_task_updated_at(db_instance_empty_fn, session, task1):
    """
    Test the updating of a task (updated_at)
    """
    # Write Task to DB
    db_instance_empty_fn.create_task(task=task1, session=session)

    # Update Task
    db_instance_empty_fn.update_task(
        session=session,
        task_id=1,
        task_title="New Title",
    )

    # Read Task from DB
    task = db_instance_empty_fn.read_task(task_id=1, session=session)

    # Check Task Updated At is greater than Created At
    assert task.updated_at > task.created_at
