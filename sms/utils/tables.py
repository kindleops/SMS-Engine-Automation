import os
from pyairtable import Table

def get_table(api_key_env: str, base_id_env: str, table_name_env: str, default_table: str):
    """
    Safely initialize a pyairtable Table.
    Falls back to AIRTABLE_API_KEY if a specific key is not provided.
    Returns None if required env vars are missing.
    """
    key = os.getenv(api_key_env) or os.getenv("AIRTABLE_API_KEY")
    base = os.getenv(base_id_env)
    table_name = os.getenv(table_name_env, default_table)
    if not (key and base):
        return None
    return Table(key, base, table_name)