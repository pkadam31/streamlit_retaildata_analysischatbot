import streamlit as st
import psycopg2
import openai
import pandas as pd
import configparser
import re

config = configparser.ConfigParser()
config.read('config.ini')
context = config['SystemContext']['context']

openai_api_key = st.secrets["openai_apikey"]
gcp_postgres_host = st.secrets["pg_host"]
gcp_postgres_user = st.secrets["pg_user"]
gcp_postgres_password = st.secrets["pg_password"]
gcp_postgres_dbname = st.secrets["pg_db"]


def run_user_sql(user_sql, conn):
    """
    Executes the user-entered SQL query and displays the results.
    :param user_sql: The user-entered SQL query.
    """
    if not validate_sql_query(user_sql):
        st.error("Disallowed SQL keywords detected. Access Denied.")
        return

    try:
        cursor = conn.cursor()
        result, df = execute_sql_query(cursor, user_sql)
        st.write("Results:")
        st.dataframe(df)

    except Exception as e:
        close_db_connection(conn, cursor=None)
        st.error(f"An error occurred: {str(e)}")


def display_message(role, content):
    """
    Display messages using Streamlit's built-in chat UI.
    """
    with st.chat_message(role):
        st.text(content)

def get_db_connection():
    """
    Establishes a connection to the database using global connection parameters.
    :return: The database connection object.
    """
    return psycopg2.connect(
        host=gcp_postgres_host,
        user=gcp_postgres_user,
        password=gcp_postgres_password,
        dbname=gcp_postgres_dbname
    )


def execute_sql_query(cursor, sql_query):
    """
    Executes the provided SQL query and returns the results.
    :param cursor: The database cursor object.
    :param sql_query: The SQL query to execute.
    :return: A tuple containing the raw result and a DataFrame representation of the result.
    """

    cursor.execute(sql_query)
    result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return result, pd.DataFrame(result, columns=column_names)


def close_db_connection(conn, cursor=None):
    """
    Closes the database connection and cursor if provided.
    :param conn: The database connection object.
    :param cursor: The database cursor object. Default is None.
    """

    if cursor:
        cursor.close()
    if conn:
        conn.close()


def get_sql_from_codex(user_query):
    """
    Generates an SQL query based on the user's input using OpenAI.
    :param user_query: The user's input query.
    :return: The generated SQL query.
    """

    user_query = "Generate a SQL query to " + user_query + ". Your response must not contain anything other than the query " \
                                                           "- not even 'Sure' or other basic english responses. "

    openai.api_key = openai_api_key
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": context},
            {"role": "user", "content": user_query},
        ],
        temperature=0.3,  # Lower temperature to reduce randomness
        max_tokens=50
    )

    return response['choices'][0]['message']['content']


def validate_sql_query(sql_query):
    """
    Validates the SQL query to ensure it doesn't contain any potentially dangerous operations or characters.
    :param sql_query: The SQL query to validate.
    :return: Boolean indicating whether the query is safe, and a sanitized query or error message.
    """

    disallowed_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', ';', '--', 'CREATE', 'ALTER']
    for keyword in disallowed_keywords:
        if re.search(f'\\b{keyword}\\b', sql_query, re.IGNORECASE):
            return False, f"Disallowed SQL keyword detected. Access denied."

    return True


def call_chatbot(user_query, conn):
    """
    Processes the user's query to generate, execute an SQL query, and display the results.
    :param user_query: The user's input query.
    """

    openai.api_key = openai_api_key
    sql_query = get_sql_from_codex(user_query)  # Generate SQL
    if not validate_sql_query(sql_query):
        close_db_connection(conn, cursor=None)
        raise ValueError("Keywords or characters detected that could trigger an attack")

    try:
        # Execute SQL Query
        cursor = conn.cursor()
        result, df = execute_sql_query(cursor, sql_query)

        response_with_results = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": context},
                {"role": "user", "content": user_query},
                {"role": "assistant", "content": f"Results: {result}"}
            ]
        )

        interpretation = response_with_results['choices'][0]['message']['content']

        # Display results in Streamlit
        with st.expander("Display SQL", expanded=False):
            st.write("SQL Query:", sql_query)
        st.write("Explanation:", interpretation)
        st.write("Results:")
        st.dataframe(df)

    except Exception as e:
        close_db_connection(conn, cursor=None)
        st.write("An error occurred:", str(e))


if __name__ == "__main__":

    st.title("Jarvis")
    st.subheader("Empowering your superhero employees to run before they can walk")  # Added subtitle
    conn = get_db_connection()

    user_query = st.text_input("Enter your question: What information do you seek from our DB today?",
                               key="chat_input")
    if user_query:
        display_message("user", user_query)
        call_chatbot(user_query, conn)

    user_sql = st.text_input("Don't need Jarvis? Run your own SQL here-", key="sql_input")
    if user_sql:
        run_user_sql(user_sql, conn)

    close_db_connection(conn, cursor=None)
