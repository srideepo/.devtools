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
    raw_sql = """SET IDENTITY_INSERT [Reference].[Tasks] ON;
            INSERT INTO [Reference].[Tasks](id, title, description, status, created_at, updated_at) values (100, 'Go to the Gym', 'Visit Gym at 09:00', 'NOT_STARTED', '2024-01-01', '2024-01-01');
            SET IDENTITY_INSERT [Reference].[Tasks] OFF;"""
    db_instance_empty_mod.execute_sql(session, raw_sql)
    #create_task(task=task1, session=session)

    # Read Task from DB
    task = db_instance_current_mod.read_task(task_id=100, session=session)
    assert task.id == 100

