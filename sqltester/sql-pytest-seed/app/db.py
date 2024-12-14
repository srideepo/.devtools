"""
Database Operations for the Task Manager
"""
import os
from typing import List
from datetime import datetime, timezone
from sqlmodel import SQLModel, create_engine, Session, select, Table, MetaData
from app.model import Tasks, TaskStatus
from app.exceptions import TaskNotFoundError
from app.logger import create_logger
from sqlalchemy import text, quoted_name

# Extract the filename without extension
filename = os.path.splitext(os.path.basename(__file__))[0]

logger = create_logger(logger_name=filename)


class DB:
    def __init__(self):
        # Create DB Engine
        #self.engine = create_engine("sqlite:///database.db", echo=True)
        self.engine = create_engine(
            "mssql+pyodbc://sa:Passw0rd@localhost:1433/testdb?"
            "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
        
        # Create schema if it doesn't exist already
        SQLModel.metadata.create_all(self.engine)
        
    def create_task2(self, task: Tasks, session: Session) -> None:
        """
        Create a task in the database

        Args:
            task (Tasks): The task to create
            Session (Session): The database session

        Returns:
            int: The ID of the created task
        """
        # Write to database
        logger.info("Creating Task in DB")
        session.execute(text("""
            SET IDENTITY_INSERT tasks ON;
            INSERT INTO tasks(id, title, description, status, created_at, updated_at) values (100, 'Go to the Gym', 'Visit Gym at 09:00', 'NOT_STARTED', '2024-01-01', '2024-01-01');
            SET IDENTITY_INSERT tasks OFF;
            """))
        session.commit()

    def create_task(self, task: Tasks, session: Session) -> None:
        """
        Create a task in the database

        Args:
            task (Tasks): The task to create
            Session (Session): The database session

        Returns:
            int: The ID of the created task
        """
        # Write to database
        logger.info("Creating Task in DB")
        session.add(task)
        session.commit()

    def read_task(self, task_id: int, session: Session) -> Tasks:
        """
        Read a task from the database

        Args:
            task_id (int): The ID of the task to read
            session (Session): The database session

        Returns:
            Tasks: The task
        """
        # Read a task from the database
        logger.info(f"Reading Task with ID {task_id} from DB")
        statement = select(Tasks).where(Tasks.id == task_id)
        result: Tasks = session.exec(statement).first()
        if result:
            return result
        else:
            logger.error("Task not found")
            raise TaskNotFoundError(f"Task with ID {task_id} not found")

    def read_tasks(self, session: Session) -> List[Tasks]:
        """
        Read all tasks from the database

        Args:
            session (Session): The database session

        Returns:
            List[Tasks]: A list of all tasks
        """
        # Read all tasks from the database
        logger.info("Reading all Tasks from DB")
        statement = select(Tasks)
        results = session.exec(statement)
        return [r for r in results]

    def update_task(
        self,
        session: Session,
        task_id: str,
        task_title: str = None,
        task_description: str = None,
        task_status: TaskStatus = None,
    ) -> None:
        """
        Update a task in the database

        Args:

            session (Session): The database session
            task_id (str): The ID of the task to update
            task_title (str, optional): The title of the task. Defaults to None.
            task_description (str, optional): The description of the task. Defaults to None.
            task_status (TaskStatus, optional): The status of the task. Defaults to None.

        Returns:

            None
        """
        # Update a task in the database
        logger.info(f"Updating Task with ID {task_id} in DB")

        # Get the task
        statement = select(Tasks).where(Tasks.id == task_id)
        result = session.exec(statement).first()

        if result:
            # Update the task
            if task_title:
                result.title = task_title

            if task_description:
                result.description = task_description

            if task_status:
                result.status = task_status

            updated_at = datetime.now(timezone.utc)
            result.updated_at = updated_at

            session.add(result)
            session.commit()
            logger.info(f"Updated Task with ID {task_id} in DB")
        else:
            logger.error(f"Task with ID {task_id} not found")
            raise TaskNotFoundError(f"Task with ID {task_id} not found")

    def delete_task(self, session: Session, task_id: int) -> None:
        """
        Delete a task from the database

        Args:
            session (Session): The database session
            task_id (int): The ID of the task to delete

        Returns:
            None
        """
        # Delete a task from the database
        logger.info("Deleting Task with ID {task_id} from DB")
        statement = select(Tasks).where(Tasks.id == task_id)
        results = session.exec(statement)
        task = results.first()

        if task:
            # Delete the task
            session.delete(task)
            session.commit()

            # Confirm the deletion
            results_post_delete = session.exec(statement)
            task_post_delete = results_post_delete.first()

            if task_post_delete is None:
                logger.info(f"Task with ID {task_id} was confirmed deleted")
        else:
            logger.error(f"Task with ID {task_id} not found")
            raise TaskNotFoundError(f"Task with ID {task_id} not found")

    def delete_all_tasks(self, session: Session) -> None:
        """
        Delete all tasks from the database

        Args:
            session (Session): The database session

        Returns:
            None
        """

        # Delete all tasks from the database
        logger.info("Deleting all Tasks from DB")
        statement = select(Tasks)
        results = session.exec(statement)

        #DBCC CHECKIDENT ('tasks', RESEED, 0);

        for task in results:
            session.delete(task)
            session.commit()

        metadata_obj = MetaData()
        self.reseed_tables(session, [Table('tasks', metadata_obj, schema="Reference")])
        #self.execute_sql(session, "DBCC CHECKIDENT ('tasks', RESEED, 0)")

        # Confirm the deletion
        results_post_delete = session.exec(statement)
        tasks_post_delete = results_post_delete.all()

        if tasks_post_delete == []:
            logger.info("All Tasks were confirmed deleted")
        else:
            logger.error("All Tasks were not deleted")
            raise Exception("All Tasks were not deleted")

    def reseed_tables(self, session, tables: List):
        for _table in tables:
            #mssql syntax to reseed identity
            self.execute_sql(session, f"DBCC CHECKIDENT ('{quoted_name(_table.schema, True)}.{quoted_name(_table.name, True)}', RESEED, 0)")        

    def execute_sql(self, session: Session, sql: str):
        _sql = text(sql)
        result = session.execute(_sql)
        return result
