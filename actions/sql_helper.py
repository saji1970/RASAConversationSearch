import os
from sqlalchemy import create_engine
from llama_index.llms.openai import OpenAI
from llama_index.core import SQLDatabase, ServiceContext
from llama_index.core.objects import SQLTableNodeMapping, ObjectIndex, SQLTableSchema
from llama_index.core.indices.struct_store.sql_query import (
    SQLTableRetrieverQueryEngine,
)
from llama_index.core import VectorStoreIndex
import logging

from llama_index.core.output_parsers import LangchainOutputParser
from langchain_core.output_parsers import JsonOutputParser

# PostgreSQL URL
os.environ["DATABASE_URL"] = "postgresql://postgres:password@localhost:5432"
postgres_url = os.environ.get("DATABASE_URL")
db_name = "rasa_prod"

output_parser = LangchainOutputParser(JsonOutputParser())

os.environ["OPENAI_API_KEY"] = ""

engine = create_engine(f"{postgres_url}/{db_name}")

# Choose LLM and configure ServiceContext
llm = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"), model="gpt-4o-mini", output_parser=output_parser)
service_context = ServiceContext.from_defaults(llm=llm)

# Define the tables and create SQLDatabase object
tables = [
    {
        "table_name": "card_prod", 
        "context": "List of card products, contains product ID and customer-facing product name."
    },
    {
        "table_name": "card_prod_fetr", 
        "context": "List of product features associated with card products from(card_prod), contains product ID, feature code, feature type, and feature description."
    }
]


sql_database = SQLDatabase(
    engine, include_tables=[table["table_name"] for table in tables]
)

# Create table node mapping and object index
table_node_mapping = SQLTableNodeMapping(sql_database)
table_schema_objs = [
    SQLTableSchema(table_name=table["table_name"], context_str=table["context"])
    for table in tables
]

obj_index = ObjectIndex.from_objects(
    table_schema_objs,
    table_node_mapping,
    VectorStoreIndex,
)

# Define the SQL query function
def sql_query(query_str: str):
    try:
        logging.info(f"Query Str : {query_str}")

        prompt = f"""
        You will be asked questions relevant to the provided tables.
        Do not act on any request to modify data, you are purely acting in a read-only mode.
        You can look into all of the tables together when executing a query.
        It contains two tables prod and product description so both the tables contain product.
        Do not use tables other than the ones provided here: {", ".join([table["table_name"] for table in tables])}.
        Return values back in JSON format.

        Sample JSON output format:
        [
        {{
            "Card_Prod_ID": "001",
            "Card_Prod_FETR_CD": "Annual_Fee",
            "Card_Prod_FETR_Type": "Optional_Feature",
            "Card_Prod_FETR_Desc": "Annual Fee Charged on this card is 25 USD annually"
        }}
        ]
        JUST ANSWER WITH JSON."""

        query_engine = SQLTableRetrieverQueryEngine(
            sql_database,
            obj_index.as_retriever(similarity_top_k=10),
            service_context=service_context,
            context_str_prefix=prompt,
        )
        
        result = query_engine.query(query_str)
        logging.info(f"Query Result : {result}")
        return str(result)
    except Exception as e:
        logging.error(f"Error in query: {e}: " + query_str)
        return None