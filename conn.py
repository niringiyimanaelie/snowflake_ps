import os
import snowflake.connector
from snowflake.connector import DatabaseError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def _format_private_key(key_content):
    """Convert PEM private key to DER format required by Snowflake."""
    try:
        # If the key is already in DER format, try to load it directly
        if isinstance(key_content, str):
            key_content = key_content.encode()
        try:
            p_key = serialization.load_der_private_key(
                key_content,
                password=None,
                backend=default_backend()
            )
            return key_content  # Already in DER format
        except ValueError:
            # If DER load fails, assume it's PEM and convert
            p_key = serialization.load_pem_private_key(
                key_content,
                password=None,
                backend=default_backend()
            )
        
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    except Exception as e:
        raise ValueError(f"Failed to process private key: {str(e)}")

def get_snowflake_connection():
    # Base connection parameters
    conn_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'authenticator': 'SNOWFLAKE_JWT'  # Always use JWT authentication
    }

    try:
        # Get private key directly from environment variable
        private_key = os.getenv('SNOWFLAKE_PRIVATE_KEY')
        if not private_key:
            raise ValueError("SNOWFLAKE_PRIVATE_KEY not set")
            
        conn_params['private_key'] = _format_private_key(private_key)
        return snowflake.connector.connect(**conn_params)
    
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}")

def fetch_training_data():
    conn = get_snowflake_connection()
    query = "SELECT * FROM PROPENSITY_TRAINING_DATA"
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return data, columns

def fetch_predict_data():
    conn = get_snowflake_connection()
    query = "SELECT * FROM PROPENSITY_PREDICT_DATA"
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return data, columns
