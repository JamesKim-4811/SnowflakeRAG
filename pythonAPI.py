# import os
# from dotenv import load_dotenv
# import pprint

# load_dotenv()

# from datetime import timedelta

# from snowflake.snowpark import Session
# from snowflake.snowpark.functions import col
# from snowflake.core import Root, CreateMode
# from snowflake.core.database import Database
# from snowflake.core.schema import Schema
# from snowflake.core.stage import Stage
# from snowflake.core.table import Table, TableColumn, PrimaryKey
# from snowflake.core.task import StoredProcedureCall, Task
# from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask
# from snowflake.core.warehouse import Warehouse

# CONNECTION_PARAMETERS = {
#     "account": os.environ["SNOWFLAKE_ACCOUNT"],
#     "user": os.environ["SNOWFLAKE_USER"],
#     "password": os.environ["SNOWFLAKE_USER_PASSWORD"],
#     "role": "ACCOUNTADMIN",
#     "database": "CC_QUICKSTART_CORTEX_SEARCH_DOCS",
#     "warehouse": "COMPUTE_WH",
#     "schema": "DATA",
# }

# # from snowflake.core import Root
# # from snowflake.snowpark import Session

# session = Session.builder.configs(CONNECTION_PARAMETERS).create()
# root = Root(session)

# database = root.databases.create(
#   Database(
#     name="PYTHON_API_DB"),
#     mode=CreateMode.or_replace
#   )

# schema = database.schemas.create(
#   Schema(
#     name="PYTHON_API_SCHEMA"),
#     mode=CreateMode.or_replace,
#   )

# table = schema.tables.create(
#   Table(
#     name="PYTHON_API_TABLE",
#     columns=[
#       TableColumn(
#         name="TEMPERATURE",
#         datatype="int",
#         nullable=False,
#       ),
#       TableColumn(
#         name="LOCATION",
#         datatype="string",
#       ),
#     ],
#   ),
# mode=CreateMode.or_replace
# )

# table_details = table.fetch()

# pprint.pp(table_details.to_dict())

# table_details.columns.append(
#     TableColumn(
#       name="elevation",
#       datatype="int",
#       nullable=False,
#       constraints=[PrimaryKey()],
#     )
# )

# table.create_or_alter(table_details)

# pprint.pp(table.fetch().to_dict())


# retrieval
import os
from dotenv import load_dotenv
import pprint

load_dotenv()

from snowflake.core import Root
from snowflake.snowpark import Session

CONNECTION_PARAMETERS = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_USER_PASSWORD"],
    "role": "PUBLIC",
    "database": "CORTEX_SEARCH_DB",
    "warehouse": "cortex_search_wh",
    "schema": "SERVICES",
}

session = Session.builder.configs(CONNECTION_PARAMETERS).create()
root = Root(session)

transcript_search_service = (root
  .databases["cortex_search_db"]
  .schemas["services"]
  .cortex_search_services["transcript_search_service"]
)

resp = transcript_search_service.search(
  query="internet",
  columns=["transcript_text", "region"],
  filter={"@eq": {"region": "North America"} },
  limit=1
)
print(resp.to_json())