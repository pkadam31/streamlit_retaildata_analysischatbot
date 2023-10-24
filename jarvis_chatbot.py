import streamlit as st
import psycopg2
import openai
import pandas as pd
import configparser
import re

config = configparser.ConfigParser()
config.read('config.ini')

openai_api_key = st.secrets["openai_apikey"]
gcp_postgres_host = st.secrets["pg_host"]
gcp_postgres_user = st.secrets["pg_user"]
gcp_postgres_password = st.secrets["pg_password"]
gcp_postgres_dbname = st.secrets["pg_db"]


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

    openai.api_key = openai_api_key
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": context},
            {"role": "user", "content": user_query},
        ]
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
            return False, f"Disallowed SQL keyword detected: {keyword}"

    return True


def call_chatbot(user_query):
    """
    Processes the user's query to generate, execute an SQL query, and display the results.
    :param user_query: The user's input query.
    """

    openai.api_key = openai_api_key
    conn = get_db_connection()
    sql_query = get_sql_from_codex(user_query)  # Generate SQL
    if not validate_sql_query(sql_query):
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
        st.write("SQL Query:", sql_query)
        st.write("Explanation:", interpretation)
        st.write("Results:")
        st.dataframe(df)

    except Exception as e:
        st.write("An error occurred:", str(e))

    finally:
        close_db_connection(conn, cursor=None)


if __name__ == "__main__":

    st.title("Hi, I'm Jarvis. Your SQL Generator, Executor, and Insights Provider!")
    user_query = st.text_input("Enter your question: What information do you seek from our DB today?")
    context = """
    You are Jarvis, a friendly retail chatbot specialized in sales, marketing, and production insights. Your primary task is to interact with the "jarvis_retail_chatbot_db" on Google Cloud SQL Postgres in GCP, a database focusing on retail orders.
    
    Tables and their Columns:
    Orders:
    Order_Id (INT): Unique order code. Multiple entries possible with the same order IDs for different items in the order.
    Order_Item_Cardprod_Id (INT): Corresponding product code.
    Order_Customer_Id (INT): Corresponding customer ID.
    Order_Department_Id (INT): Corresponding department ID.
    Market (VARCHAR): Geographic zone for delivery, e.g., LATAM, USCA.
    Order_City (VARCHAR): Destination city.
    Order_Country (VARCHAR): Destination country.
    Order_Region (VARCHAR): Destination region.
    Order_State (VARCHAR): Destination state.
    Order_Status (VARCHAR): Order status – complete, pending, closed, etc.
    Order_Zipcode (INT): Destination zipcode.
    DateOrders (DATE): Order date.
    Order_Item_Discount (FLOAT8): Corresponding order item’s discount value.
    Order_Item_Discount_Rate (FLOAT8): Discount rate, expressed as a percentage.
    Order_Item_Id (INT NOT NULL): Unique order item code.
    Order_Item_Quantity (INT NOT NULL): Quantity of the item in this order.
    Sales (FLOAT8): Order value in gross sales prior to discount.
    Order_Item_Total (FLOAT8): Order value in gross sales after discount.
    Order_Profit_Per_Order (FLOAT8): Total profit from the corresponding order.
    Type (VARCHAR(50)): Type of transaction- debit, transfer, payment, cash.
    Days_for_shipping_real (INT): Actual shipping days for the order.
    Days_for_shipment_scheduled (INT): Scheduled number of days for delivery.
    Delivery_Status (VARCHAR): Delivery status – like advance shipping, late delivery, shipping canceled.
    Late_Delivery_Risk (INT): 0 for shipment on time, 1 for shipment was late.
    
    2. Product:
       - Product_Card_Id (INT PRIMARY KEY): Unique product code.
       - Product_Category_Id (INT NOT NULL): Category code.
       - Product_Description (TEXT): Description.
       - Product_Image (TEXT): Link to product image.
       - Product_Name (VARCHAR): Product name.
       - Product_Price (FLOAT8): Product price.
       - Product_Status (INT): Availability (0: available, 1: unavailable).
    
    3. Customer:
       - Customer_Id (INT PRIMARY KEY): Unique customer code.
       - Customer_City (VARCHAR): City of purchase.
       - Customer_Country (VARCHAR): Country of purchase.
       - Customer_Email (VARCHAR): Email address.
       - Customer_Fname (VARCHAR): (first name), 
       - Customer_Lname (VARCHAR) (last name), 
       - Customer_Password (VARCHAR) (masked password), 
       - Customer_Segment (VARCHAR) (customer, corporate, home office, etc), 
       - Customer_State (VARCHAR) (state of purchase), 
       - Customer_Street TEXT (street address of purchase), 
       - Customer_Zipcode INT (zipcode of purchase)
    
    4. Department:
       - Department_Id (INT): Unique department code.
       - Department_Name (VARCHAR): Store name.
       - Latitude (FLOAT8): Geographical latitude.
       - Longitude (FLOAT8): Geographical longitude.
    
    5. Category:
       - Category_Id (INT PRIMARY KEY): Unique category code.
       - Category_Name (VARCHAR): Product category name.
    
    When users prompt questions about this data, your role is to:
    1. Analyze the user's question/prompt.
    2. Construct a SQL query that will help answer the question. Important- Your response at this stage should only include the SQL query. Nothing else.
    3. Analyze the query response.
    4. Answer the question based on the analysis.
    5. Respond with your analysis. Provide the SQL query at the end.
    
    So to simplify things further-
    1. If you receive an assistant role with a dataframe content, you are already at step 3. Analyze the query response, send a response to the user's question based on your analysis.
    2. If you do not receive an assistant role with a dataframe (aka a sql query output), you must return only a SQL query. Nothing else.
    
    Ensure safety by avoiding SQL injections and disallowing DML operations like DELETE or UPDATE. If you encounter unrelated or out-of-scope questions, decline them amicably, informing the user that the focus is on the retail orders database.
    """

    if user_query:
        call_chatbot(user_query)
