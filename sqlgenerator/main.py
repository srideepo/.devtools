import pyodbc
from sqlalchemy import create_engine, Column, String, Integer, Table, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from uuid import uuid4
from sqlalchemy.schema import CreateTable
import pandas as pd

_schemafile = './metadata.xlsx'
_dfschema = pd.read_excel(_schemafile, index_col=None, keep_default_na=False)

# Set up the engine and base
engine = create_engine(
            "mssql+pyodbc://sa:Passw0rd@localhost:1433/testdb?"
            "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
            )
Base = declarative_base()

def create_dynamic_model(table):
    class DynamicModel(Base):
        __table__ = table
    return DynamicModel

for _table in _dfschema.sort_values('IX')['TABLE_NAME'].unique():
    _dftable = _dfschema[_dfschema['TABLE_NAME'] == _table]
    _columns = []
    for _rix, _row in _dftable.iterrows():
        _primary_key = True if _row['IS_PRIMARY_KEY'] == 1 else False
        _data_type = Integer
        match _row['DATA_TYPE'].upper():
            case 'CHAR' | 'NCHAR' | 'VARCHAR' | 'NVARCHAR':
                _data_type = String if _row['CHARACTER_MAXIMUM_LENGTH'] == -1 else String(int(_row['CHARACTER_MAXIMUM_LENGTH']))
            case 'DATETIME' | 'DATETIME2':
                _data_type = DateTime
        if _row['FOREIGN_KEY_COLUMN'] == '':
            _columns.append(Column(_row['COLUMN_NAME'], _data_type, primary_key=_primary_key))
        else:
            _columns.append(Column(_row['COLUMN_NAME'], _data_type, ForeignKey(f"{_row['FOREIGN_KEY_TABLE']}.{_row['FOREIGN_KEY_COLUMN']}"), primary_key=_primary_key))

    _tabledef = Table(
        _table,
        Base.metadata,
        *_columns
    )
    DynamicModel = create_dynamic_model(_tabledef)
    #print (_dftable)

# Create the tables in the database
Base.metadata.create_all(engine)

for table in Base.metadata.sorted_tables:
    print(CreateTable(table).compile(engine), ';')