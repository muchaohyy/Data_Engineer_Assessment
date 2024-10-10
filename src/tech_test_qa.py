import psycopg2
import pandas as pd
import datetime

class PostgresDB:
    def __init__(self, host, database, user, password, port=5432):
        """Initialize the database connection parameters."""
        self._connection_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self._connection = None

    def _connect(self):
        """Establishes the connection to the PostgreSQL database."""
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(**self._connection_params)
                print("PostgreSQL connection established.")
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Error while connecting: {error}")
                raise

    def run_query(self, query, params=None, as_dataframe=False):
        """
        Executes a given SQL query.
        
        :param query: SQL query to run.
        :param params: Optional parameters for the query.
        :param as_dataframe: If True, return result as a pandas DataFrame (for SELECT queries).
        :return: Query result or pandas DataFrame if as_dataframe=True.
        """
        result = None
        self._connect()  # Ensure connection is established before running the query
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, params)
                if cursor.description:  # Check if the query returns a result (SELECT)
                    columns = [desc[0] for desc in cursor.description]  # Get column names
                    rows = cursor.fetchall()  # Fetch all rows for SELECT queries
                    if as_dataframe:
                        result = pd.DataFrame(rows, columns=columns)  # Convert to DataFrame
                    else:
                        result = rows  # Return rows as a list of tuples
                self._connection.commit()  # Commit any changes (for INSERT/UPDATE)
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error executing query: {error}")
            self._connection.rollback()  # Rollback in case of error
        return result

    def close(self):
        """Closes the database connection."""
        if self._connection is not None:
            self._connection.close()
            print("PostgreSQL connection is closed.")

def print_log(message, end='\n'):
    """
    Print a message with a timestamp. Handles str and dict types.

    Parameters:
        message (str or dict): The message to print. Can be a string or a dictionary.
        end (str): The string appended after the message. Defaults to newline character.
    """
    # Get the current timestamp
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Check the type of the message
    if isinstance(message, dict):
        # Convert dict to a formatted JSON string
        message = json.dumps(message, indent=4)  # Pretty-print with 4 spaces indentation
    
    # Print the message with timestamp
    print(f"[{timestamp}] {message}", end=end)

def handle_duplicates(df, subset=None, keep='first', drop=False):
    """
    Identify and optionally drop duplicates in a pandas DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame to check for duplicates.
        subset (list, optional): Column names to consider for identifying duplicates.
                                  If None, all columns are used.
        keep (str): Determines which duplicates to keep. Options are:
                     - 'first': keep the first occurrence (default)
                     - 'last': keep the last occurrence
                     - False: drop all duplicates
        drop (bool): If True, drop duplicates from the DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates handled based on the given parameters.
        pd.Series: Series indicating whether each row is a duplicate.
    """
    # Identify duplicates
    duplicates = df.duplicated(subset=subset, keep=keep)

    num_duplicates = duplicates.sum()

    # Drop duplicates if requested
    if drop:
        df = df[~duplicates]  # Keep only non-duplicates

    return df, duplicates, num_duplicates

    # Handle duplicates in all columns
    # df_cleaned_all, duplicates_all = handle_duplicates(df, drop=False)

    # Handle duplicates in specific columns (e.g., 'A' and 'B')
    # df_cleaned_subset, duplicates_subset = handle_duplicates(df, subset=['A', 'B'], drop=False)

def check_binary_column(df, column_name):
    """
    Check if a specified column in a DataFrame contains only 0s and 1s.

    Parameters:
        df (pd.DataFrame): The DataFrame to check.
        column_name (str): The name of the column to check.

    Returns:
        bool: True if the column contains only 0s and 1s, otherwise False.
    """
    # Check if the column exists in the DataFrame
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")

    # Check if all values in the column are either 0 or 1
    is_binary = df[column_name].isin([0, 1]).all()
    
    return is_binary

def check_column_is_numerical(df, column_name):
    """
    Check if a specified column in a DataFrame contains only numerical values.

    Parameters:
        df (pd.DataFrame): The DataFrame to check.
        column_name (str): The name of the column to check.

    Returns:
        bool: True if the column contains only numerical values, otherwise False.
    """
    # Check if the column exists in the DataFrame
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")

    # Check if the column is of numerical type
    is_numerical = pd.api.types.is_numeric_dtype(df[column_name])
    
    return is_numerical

def check_column_is_timestamp(df, column_name):
    """
    Check if a specified column in a DataFrame is of timestamp type.

    Parameters:
        df (pd.DataFrame): The DataFrame to check.
        column_name (str): The name of the column to check.

    Returns:
        bool: True if the column is of timestamp type, otherwise False.
    """
    # Check if the column exists in the DataFrame
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")

    # Check if the column is of timestamp type
    is_timestamp = pd.api.types.is_datetime64_any_dtype(df[column_name])
    
    return is_timestamp

def check_mandatory_columns(df, columns=None):
    """
    Check if all specified columns in a DataFrame are mandatory (i.e., contain no null values).

    Parameters:
        df (pd.DataFrame): The DataFrame to check.
        columns (list, optional): List of column names to check. If None, checks all columns.

    Returns:
        bool: True if all specified columns are non-null, otherwise False.
        dict: A dictionary with column names as keys and boolean values indicating their null status.
    """
    # If no specific columns are provided, check all columns
    if columns is None:
        columns = df.columns.tolist()

    # Check if all specified columns exist in the DataFrame
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Columns {missing_columns} do not exist in the DataFrame.")

    # Check for null values in the specified columns
    null_status = {col: df[col].isnull().any() for col in columns}
    
    # Determine if all specified columns are mandatory
    all_mandatory = all(not has_null for has_null in null_status.values())
    
    return all_mandatory, null_status

def check_column_greater_than(df, column_a, column_b=None):
    """
    Check if the values in one column are always greater than another column or zero.

    Parameters:
        df (pd.DataFrame): The DataFrame to check.
        column_a (str): The name of the column to check if it is greater.
        column_b (str, optional): The name of the column to compare against. If None, compares against zero.

    Returns:
        bool: True if column_a is always greater than column_b or zero, otherwise False.
    """
    # Check if the columns exist in the DataFrame
    if column_a not in df.columns:
        raise ValueError(f"Column '{column_a}' does not exist in the DataFrame.")
    if column_b is not None and column_b not in df.columns:
        raise ValueError(f"Column '{column_b}' does not exist in the DataFrame.")

    # Compare column_a with column_b or zero
    if column_b is not None:
        condition = df[column_a] > df[column_b]
    else:
        condition = df[column_a] > 0

    # Return True if the condition is met for all rows
    return condition.all()

def foreign_key_check(df_fk, fk_columns, df_pk, pk_columns):
    """
    Perform a foreign key check between two DataFrames.

    Parameters:
        df_fk (pd.DataFrame): DataFrame containing the foreign key columns.
        fk_columns (list): A list of column names in df_fk that are foreign keys.
        df_pk (pd.DataFrame): DataFrame containing the primary key columns.
        pk_columns (list): A list of column names in df_pk that are primary keys.

    Returns:
        Tuple[bool, pd.DataFrame]: 
        - True if all foreign key values are present in the primary key DataFrame, otherwise False.
        - A DataFrame containing mismatched foreign key values if any.
    """
    # Check if the specified columns exist in df_fk
    for col in fk_columns:
        if col not in df_fk.columns:
            raise ValueError(f"Column '{col}' does not exist in the foreign key DataFrame.")

    # Check if the specified columns exist in df_pk
    for col in pk_columns:
        if col not in df_pk.columns:
            raise ValueError(f"Column '{col}' does not exist in the primary key DataFrame.")

    # Create a set of primary key combinations
    primary_keys = set(zip(*(df_pk[col] for col in pk_columns)))

    # Create a set of foreign key combinations
    foreign_keys = set(zip(*(df_fk[col] for col in fk_columns)))

    # Find mismatched foreign key combinations
    mismatches = foreign_keys - primary_keys
    
    # Return True if all foreign keys are valid, otherwise False and the mismatches
    return len(mismatches) == 0, pd.DataFrame(list(mismatches), columns=fk_columns)

def check_users(db, df):
    print_log("==========================================Start checking users==========================================")

    #duplicates check
    print_log('1. Check duplicates in users table on all columns')
    df_cleaned_all, duplicates_all, num_duplicates = handle_duplicates(df, drop=False)
    print_log(f"Number of duplicates found: {num_duplicates}")

    #data type check
    print_log('2. Check if column enable only contains 1 and 0')
    result_enable = check_binary_column(df, 'enable')
    print_log(f"Column 'enable' contains only 0s and 1s: {result_enable}")

    #mandatory type check
    print_log("3. Check if columns - 'login_hash', 'server_hash', 'country_hash', 'currency', 'enable' are mandatory")
    columns_mandatory, null_status = check_mandatory_columns(df, columns=['login_hash', 'server_hash', 'country_hash', 'currency', 'enable'])
    print_log(f"Columns - 'login_hash', 'server_hash', 'country_hash', 'currency', 'enable' are mandatory: {columns_mandatory}")
    print_log("Null status for columns:")
    print(null_status)

    print_log("==========================================Finish checking users==========================================")

def check_trades(db, df):
    print_log("==========================================Start checking trades==========================================")

    #duplicates check
    print_log('1. Check duplicates in users trades on all columns')
    df_cleaned_all, duplicates_all, num_duplicates = handle_duplicates(df, drop=False)
    print_log(f"Number of duplicates found: {num_duplicates}")

    print_log("2. Check duplicates in users trades on columns - 'login_hash', 'ticket_hash', 'server_hash', 'open_time'")
    df_cleaned_all, duplicates_all, num_duplicates = handle_duplicates(df, subset=['login_hash', 'ticket_hash', 'server_hash', 'open_time'], drop=False)
    print_log(f"Number of duplicates found: {num_duplicates}")

    #data type check
    print_log("3. Check if column 'digits' is numerical")
    result_digits = check_column_is_numerical(df, 'digits')
    print_log(f"Column 'digits' is of numerical type: {result_digits}")

    print_log("4. Check if column 'cmd' is numerical")
    result_cmd = check_column_is_numerical(df, 'cmd')
    print_log(f"Column 'cmd' is of numerical type: {result_cmd}")

    print_log("5. Check if column 'volume' is numerical")
    result_volume = check_column_is_numerical(df, 'volume')
    print_log(f"Column 'volume' is of numerical type: {result_volume}")

    print_log('6. Check if column cmd only contains 1 and 0')
    result_cmd = check_binary_column(df, 'cmd')
    print_log(f"Column 'cmd' contains only 0s and 1s: {result_cmd}")

    print_log("7. Check if column 'open_time' is of timestamp type")
    result_open_time = check_column_is_timestamp(df, 'open_time')
    print_log(f"Column 'open_time' is of timestamp type: {result_open_time}")

    print_log("8. Check if column 'close_time' is of timestamp type")
    result_close_time = check_column_is_timestamp(df, 'close_time')
    print_log(f"Column 'close_time' is of timestamp type: {result_close_time}")

    #mandatory type check
    print_log("9. Check if columns - 'login_hash', 'ticket_hash', 'server_hash', , 'symbol', 'digits', 'cmd', 'volume', 'open_time', 'open_price', 'contractsize' are mandatory")
    columns_mandatory, null_status = check_mandatory_columns(df, columns=['login_hash', 'ticket_hash', 'server_hash', 'symbol', 'digits', 'cmd', 'volume', 'open_time', 'open_price', 'contractsize'])
    print_log(f"Columns - 'login_hash', 'ticket_hash', 'server_hash', , 'symbol', 'digits', 'cmd', 'volume', 'open_time', 'open_price', 'contractsize' are mandatory: {columns_mandatory}")
    print_log("Null status for columns:")
    print(null_status)

    #logic check
    #volume should be greater than 0
    print_log("10. Check if column 'volume' is always greater than 0")
    result_volume = check_column_greater_than(df, 'volume')
    print_log(f"Column 'volume' is always greater than 0: {result_volume}")

    #open_time should be smaller than close_time
    print_log("11. Check if column 'close_time' is always greater than column 'open_time'")
    result = check_column_greater_than(df, 'close_time', 'open_time')
    print_log(f"Column 'close_time' is always greater than column 'open_time': {result}")

    print_log("==========================================Finish checking trades==========================================")

# As we are using AWS, it is recommended to use SSM to store all the credentials and fetch them in the code
host="host"
database="database"
user="user"
password="password"

db = PostgresDB(host=host, database=database, user=user, password=password)
df_users = db.run_query("SELECT * FROM users;", as_dataframe=True)
df_trades = db.run_query("SELECT * FROM trades;", as_dataframe=True)

# check users
check_users(db, df_users)

# check trades
check_trades(db, df_trades)

#cross reference check
print_log("==========================================Start cross reference check==========================================")
#check if the values of columns - 'login_hash', 'server_hash' in trades table are also in users table
print_log("1. Check if the values of columns - 'login_hash', 'server_hash' in trades table are also in users table")
result, mismatched_keys = foreign_key_check(df_trades, ['login_hash', 'server_hash'], df_users, ['login_hash', 'server_hash'])

print_log(f"All values of columns - 'login_hash', 'server_hash' are valid: {result}")
print_log("Mismatched values of columns - 'login_hash', 'server_hash':")
print(mismatched_keys)

#If we have a currency reference table, we can use it to check if currency in users table is valid or not
print_log("==========================================Finish cross reference check==========================================")

# Close the connection when done
db.close()