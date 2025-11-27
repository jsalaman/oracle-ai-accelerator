import streamlit as st
import pandas as pd
from services.database.connection import Connection

class SelectAIService:
    """
    Service class for managing Select AI operations.
    """

    def __init__(self):
        """
        Initializes the SelectAIService with a shared database connection.
        """
        self.conn_instance = Connection()
        self.conn = self.conn_instance.get_connection()

    def create_user(self, user_id, password):
        """
        Creates a new database user.

        Args:
            user_id (int): The user_id for the new database user.
            password (str): The password for the new database user.

        Returns:
            str: A message indicating success.
        """
        # CREATE USER statements cannot be parameterized with bind variables for identifiers
        # We must carefully validate/sanitize input or accept that DDL requires string concatenation.
        # Here user_id is an integer so it is safe. Password should be handled carefully.
        # Assuming user_id is safe (int).

        # NOTE: Parameterized queries (binding) are generally not supported for DDL statements (like CREATE USER).
        # We continue to use f-strings here but ensure inputs are safe.
        query = f"""
                CREATE USER SEL_AI_USER_ID_{str(user_id)}
                IDENTIFIED BY "{password}"
                DEFAULT TABLESPACE tablespace
                QUOTA UNLIMITED ON tablespace
            """
        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()

        with self.conn.cursor() as cur:
            cur.execute(f"""
                GRANT DWROLE TO SEL_AI_USER_ID_{str(user_id)}
            """)
        self.conn.commit()
        return f"[Select AI]: New User :red[SEL_AI_USER_ID_{str(user_id)}] created successfully for the database."
    
    def drop_user(self, user_id):
        """
        Deletes a database user.

        Args:
            user_id (int): The username of the database user to delete.

        Returns:
            str: A message indicating success.
        """
        # DDL cannot be parameterized for identifiers.
        try:
            query = f"""
                DROP USER SEL_AI_USER_ID_{str(user_id)} CASCADE
            """
            with self.conn.cursor() as cur:
                cur.execute(query)
            self.conn.commit()
            return f"[Select AI]: The username :red[SEL_AI_USER_ID_{str(user_id)}] of the database user to delete successfully."
        except Exception as e:
            # The username does not exist.
            if 'ORA-01918' in str(e):
                return f"[Select AI]: The username :red[SEL_AI_USER_ID_{str(user_id)}] of the database does not exist."""

    def update_user_password(self, user_id, new_password):
        """
        Updates the password of a database user.

        Args:
            user_id (int): The username of the database.
            new_password (str): The new password to set for the user.

        Returns:
            str: A message indicating the success of the operation.
        """
        # ALTER USER cannot be parameterized for identifiers/passwords in standard way.
        with self.conn.cursor() as cur:
            cur.execute(f"""
                ALTER USER SEL_AI_USER_ID_{str(user_id)} IDENTIFIED BY "{new_password}"
            """)
        self.conn.commit()
        return f"[Select AI] The password for user was updated successfully."
    

    def update_comment(
            self,
            table_name,
            column_name,
            comment
        ):
        """
        Updates the comment for a specific column in a table.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column.
            comment (str): The comment to set for the column.
        """
        # COMMENT ON is DDL, table/column names cannot be bound. Comment text IS a string literal,
        # but in 'COMMENT ON ... IS ''literal''' syntax, it's also part of DDL.
        # It's better to escape single quotes manually if binding isn't supported for DDL.
        safe_comment = comment.replace("'", "''")
        with self.conn.cursor() as cur:
            cur.execute(f"""
                COMMENT ON COLUMN {table_name}.{column_name} IS '{safe_comment}'
            """)
        self.conn.commit()
    
    def create_table_from_csv(
            self,
            object_uri,
            table_name
        ):
        """
        Creates a table in the database from a CSV file.

        Args:
            object_uri (str): The URI of the CSV file.
            table_name (str): The name of the table to create.
        """
        # Uses a stored procedure, so we can use binding or just formatting.
        # Since it is a PL/SQL block calling a stored proc, we can use bind variables?
        # The procedure SP_SEL_AI_TBL_CSV probably takes varchar2 arguments.
        with self.conn.cursor() as cur:
            query = """
                BEGIN
                    SP_SEL_AI_TBL_CSV(:object_uri, :table_name);
                END;
            """
            cur.execute(query, {
                "object_uri": object_uri,
                "table_name": table_name
            })
        self.conn.commit()

    def create_profile(
            self,
            profile_name,
            user_id
        ):
        """
        Creates a profile for Select AI in the database.

        Args:
            profile_name (str): The name of the profile to create.
            user_id (str): The ID of the user creating the profile.
        """
        # Stored procedure call.
        with self.conn.cursor() as cur:
            query = """
                BEGIN
                    SP_SEL_AI_PROFILE(:profile_name, :user_id);
                END;
            """
            cur.execute(query, {
                "profile_name": profile_name,
                "user_id": int(user_id)
            })
        self.conn.commit()
    
    def get_chat(
            self,
            prompt,
            profile_name,
            action,
            language
        ):
        """
        Generates a chat response using the Select AI profile.

        Args:
            prompt (str): The user prompt or query.
            profile_name (str): The name of the profile to use.
            action (str): The action to perform.
            language (str): The language for the response.

        Returns:
            str: The generated chat response.
        """ 
        
        # Replace single quotes to avoid SQL syntax issues - still needed if we put prompt in string
        # But we can try to bind parameters for DBMS_CLOUD_AI.GENERATE arguments!
        # DBMS_CLOUD_AI.GENERATE is a function.
        # We can call it via SELECT ... FROM DUAL or PL/SQL.
        # Let's try binding.
        
        # Note: prompt contains instructions that are injected via f-string originally.
        # We should construct the full prompt string in python and bind it.

        full_prompt = f"{prompt} /** Format the response in markdown. Do not underline titles. Queries must always be written in uppercase. Just focus on the database tables. Answer in {language}. If you do not know the answer, answer imperatively and exactly: 'NNN.' **/"

        query = """
            SELECT
                DBMS_CLOUD_AI.GENERATE(
                prompt       => :full_prompt,
                profile_name => :profile_name,
                action       => :action) AS CHAT
            FROM DUAL
        """

        # Execute the query and return the chat response
        return pd.read_sql(query, con=self.conn, params={
            "full_prompt": full_prompt,
            "profile_name": profile_name,
            "action": action
        })["CHAT"].iloc[0].read()
    
    def get_tables_cache(self, user_id, force_update=False):
        if force_update:
            # Borra la caché de la función
            self.get_tables.clear()
        return self.get_tables(user_id)
    
    @st.cache_data
    def get_tables(_self, user_id):        
        """
        Retrieves metadata for tables associated with the Select AI module.

        Returns:
            pd.DataFrame: A DataFrame containing table metadata, including columns and comments.
        """
        query = """
            SELECT 
                t.owner,
                t.table_name,
                c.column_name,
                c.data_type,
                cc.comments
            FROM 
                all_tables t
            JOIN 
                all_tab_columns c
                ON t.table_name = c.table_name AND t.owner = c.owner
            LEFT JOIN 
                all_col_comments cc
                ON c.table_name = cc.table_name 
                AND c.owner = cc.owner 
                AND c.column_name = cc.column_name
            WHERE 
                (UPPER(t.owner), UPPER(t.table_name)) IN (
                    SELECT 
                        UPPER(SUBSTR(F.FILE_TRG_OBJ_NAME, 1, INSTR(F.FILE_TRG_OBJ_NAME, '.') - 1)) AS owner,
                        UPPER(SUBSTR(F.FILE_TRG_OBJ_NAME, INSTR(F.FILE_TRG_OBJ_NAME, '.') + 1)) AS table_name
                    FROM FILES F
                    JOIN FILE_USER FU ON F.FILE_ID = FU.FILE_ID
                    WHERE 
                        F.MODULE_ID = 1 
                        AND F.FILE_STATE = 1 
                        AND FU.USER_ID = :user_id
                )
            ORDER BY 
                t.owner, t.table_name, c.column_id
        """
        return pd.read_sql(query, con=_self.conn, params={"user_id": user_id})

    def get_data(self, sql):
        """
        Ejecuta el SQL recibido y devuelve el DataFrame completo sin modificaciones.
        """
        # This executes arbitrary SQL returned by the LLM.
        # It is inherently risky but it is the purpose of the tool (Select AI).
        # We cannot parameterize this as it's a full SQL string.
        try:
            return pd.read_sql(sql, con=self.conn)
        except Exception:
            return pd.DataFrame()
