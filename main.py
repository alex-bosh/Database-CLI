#!/usr/bin/env python
# import statements
import numpy as np
import psycopg2 as db_connect

# connecting to the database
try:
    host_name="localhost"
    db_user="postgres"
    db_password="password"
    db_name="postgres"
    connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
except Exception as e:
    print("ERROR: " + e)
    exit()

# any commit is made automatically
connection.autocommit = True

cursor = connection.cursor()







## HELPER FUNCTIONS

# Function to get user's input in the main menu
def get_input():
    print("Welcome to the Airbnb Database CLI Interface!\n\nSelect one of the following options:")
    print("1. Insert Data\n2. Delete Data\n3. Update Data\n4. Search Data\n5. Aggregate Functions\n6. Sorting\n7. Joins\n8. Grouping\n9. Subqueries\n10. Transactions\n11. Error Handling\n12. Exit\n")
    return input("=> ")

# Function to get any inputs involving conditions
def get_condition():

    insert_input = '1' # For adding a column to the condition
    used_cols = [] # Stores all of the inputted columns
    used_data = [] # Stores all of the inputted data for the columns
    operator_data = [] # Stores all of the operators needed (column OPERATOR data)
    disp_data = [] # For query execution
    del_data = [] # For print statement purposes

    while insert_input != '/stop':
        print("Type in a column to add to the condition, or type in /stop to stop:\n")
        insert_input = input("=> ")
        if insert_input != '/stop':
            used_cols.append(insert_input)
            print("Enter data for column " + insert_input + ":\n")
            data_input = input("=> ")
            used_data.append(data_input)

            print("CURRENT CONDITION: " + insert_input + " x " + data_input)
            print("Replace \'x\' above with an operator (e.g., <, >, =, LIKE):\n") 
            operator_input = input("=> ")
            operator_data.append(operator_input)
            disp_data.append(insert_input + " " + operator_input + " " + data_input)
            
            print("To add in another condition, type in \'AND\' or \'OR\'. Otherwise, type /stop:\n")
            another_input = input("=> ")
            if another_input != '/stop':
                del_data.append(insert_input + " " + operator_input + " " + data_input + " " + another_input)
            else:
                del_data.append(insert_input + " " + operator_input + " " + data_input)
                insert_input = '/stop'
        
    return insert_input, used_cols, used_data, operator_data, disp_data, del_data

# Helper function for user to select one of the eight tables
def select_table():
    print("Select the table you would like to perform the action on:")
    print("1. Profile\n2. Host\n3. Review\n4. Rating\n5. Guest\n6. Listing\n7. Room\n8. Location\n")
    the_table = 'Profile'
    user_input = input("=> ")
    if user_input == '2':
        the_table = 'Host'
    elif user_input == '3':
        the_table = 'Review'
    elif user_input == '4':
        the_table = 'Rating'
    elif user_input == '5':
        the_table = 'Guest'
    elif user_input == '6':
        the_table = 'Listing'
    elif user_input == '7':
        the_table = 'Room'
    elif user_input == '8':
        the_table = 'Location'
    return the_table




## FUNCTIONALITY FUNCTIONS

# Function for #1 CLI Implementation: Insert Data
def insert_data():
    used_cols = [] # All columns used
    used_data = [] # All data inserted for each column
    disp_data = [] # For query execution purposes
    insert_input = '1' # For column input
    data_input = '1' # For data of column input
    the_table = select_table() # Get user input for table
    print("Inserting data in table " + the_table + ".\n")
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]

    while insert_input != '/stop':
        # Display columns of table
        print("Columns: " + ', '.join(colnames))
        print("Enter a column name to add data to, or type in /stop to stop:\n")
        insert_input = input("=> ")
        if insert_input != '/stop':
            used_cols.append(insert_input)
            print("Enter data for column " + insert_input + ":\n")
            data_input = input("=> ")
            used_data.append(data_input)
            disp_data.append(insert_input + "=" + data_input)
    
    if used_cols:
        try:
            cursor.execute("INSERT INTO " + the_table + "(" + ", ".join(used_cols) + ") VALUES (" + ", ".join(used_data) + ");")
        except Exception as e:
            print("ERROR: " + e)
            exit()
        print("QUERY EXECUTED:\nINSERT INTO " + the_table + "(" + ", ".join(used_cols) + ") VALUES (" + ", ".join(used_data) + ");\n")
        
        cursor.execute("SELECT * FROM " + the_table + " WHERE " + " AND ".join(disp_data))
        result = cursor.fetchone()
        print("RESULT: ")
        print(result)
        print("")

# Function for #2 CLI Implementation: Delete Data
def delete_data():
    used_cols = [] # All columns used in conditions
    used_data = [] # All data used in conditions
    operator_data = [] # Any operators used in conditions
    del_data = [] # For query execution purposes
    disp_data = [] # For printing results
    insert_input = '1' # Inserting column inputs
    the_table = select_table() # Get user input for table to use
    print("Deleting data from table " + the_table + ".\n")
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]

    while insert_input != '/stop':
        print("Columns: " + ', '.join(colnames))
        print("DELETE FROM " + the_table + " WHERE (condition)")
 
        insert_input, used_cols, used_data, operator_data, disp_data, del_data = get_condition()
    
    if used_cols:
        try:
            cursor.execute("SELECT * FROM " + the_table + " WHERE " + " AND ".join(disp_data))
            result = cursor.fetchone()
            
            cursor.execute("DELETE FROM " + the_table + " WHERE " + " ".join(del_data) + ";")
        except Exception as e:
            print("ERROR: " + e)
            exit()

        print("QUERY EXECUTED:\nDELETE FROM " + the_table + " WHERE " + " ".join(del_data) + ");\n")
        

        print("DELETED THE FOLLOWING DATA: ")
        print(result)
        print("")

# Function for #3 CLI Implementation: Update Data
def update_data():
    update_col = '1' # Column to update
    update_data = '1' # Data to be updated
    used_cols = [] # Any used columns
    used_data = [] # Any used data
    operator_data = [] # Any used operators in conditions
    del_data = [] # For query execution purposes
    disp_data = [] # To print results of query
    insert_input = '1' # New data value for column
    the_table = select_table() # Get user input for table
    print("Updating data from table " + the_table + ".\n")
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]

    while insert_input != '/stop':
        print("Columns: " + ', '.join(colnames))
        print("UPDATE " + the_table + " SET column = newValue WHERE (condition)")
        print("Enter a column name to update in:\n")
        update_col = input("=> ")
        print("Enter new data value for column " + insert_input + ":\n")
        update_data = input("=> ")

        insert_input, used_cols, used_data, operator_data, disp_data, del_data = get_condition()
    
    if used_cols:
        
        try:
            cursor.execute("SELECT * FROM " + the_table + " WHERE " + " AND ".join(disp_data))
            result = cursor.fetchone()
            
            cursor.execute("UPDATE " + the_table + " SET " + update_col + " = " + update_data  + " WHERE " + " ".join(del_data) + ";")
            print("QUERY EXECUTED:\nUPDATE " + the_table + " SET " + update_col + " = " + update_data + " WHERE " + " ".join(del_data) + ");\n")
            
            cursor.execute("SELECT * FROM " + the_table + " WHERE " + " AND ".join(disp_data))
            result2 = cursor.fetchone()
        except Exception as e:
            print("ERROR: " + e)
            exit()

        print("UPDATED THE FOLLOWING DATA: ")
        print(result)
        print("TO")
        print(result2)

# Function for #4 CLI Implementation: Search Data
def search_data():
    used_cols = [] # For any used columns
    used_data = [] # For any used data
    operator_data = [] # For any used condition operators
    del_data = [] # For printing purposes
    disp_data = [] # For query execution purposes
    insert_input = '1' # Condition inputs from user
    the_table = select_table() # Get table input from user
    print("Searching data from table " + the_table + ".\n")
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]

    while insert_input != '/stop':
        print("Columns: " + ', '.join(colnames))
        print("SELECT * FROM " + the_table + " WHERE (condition)")
 
        insert_input, used_cols, used_data, operator_data, disp_data, del_data = get_condition()
    
    if used_cols:
        try:
            cursor.execute("SELECT * FROM " + the_table + " WHERE " + " AND ".join(disp_data) + " LIMIT 15")
        except Exception as e:
            print("ERROR: " + e)
            exit()

        print("QUERY EXECUTED:\nSELECT * FROM " + the_table + " WHERE " + " ".join(del_data) + ");\n")
        
        print("RESULT: ")
        for row in cursor.fetchall():
            print(row)
        print("Limit of 15 entries displayed for readability\n")

# Function for #5 CLI Implementation: Aggregate Functions
def aggregate_functions():
    aggregate_col = '1' # Column name input to perform aggregate function on
    aggregate_type = '1' # Type of aggregate function to be used
    the_table = select_table() # Get table input from user
    print("Searching aggregate data from table " + the_table + ".\n")
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]


    print("Columns: " + ', '.join(colnames))
    print("SELECT aggregate(column) FROM " + the_table)
    print("Enter a column name to perform the aggregate function on:\n")
    aggregate_col = input("=> ")
    print("Enter the aggregate function type to use (e.g., SUM, COUNT, MIN, MAX):\n")
    aggregate_type = input("=> ")

    try: 
        cursor.execute("SELECT " + aggregate_type + "(" + aggregate_col + ")" + " FROM " + the_table)
        result = cursor.fetchone()
    except Exception as e:
        print("ERROR: " + e)
        exit()

    print("QUERY EXECUTED:\nSELECT " + aggregate_type + "(" + aggregate_col + ")" + " FROM " + the_table + "\n")
        

    print("RESULT: ")
    print(result)
    print("")

# Function for #6 CLI Implementation: Sorting
def sorting_data():
    insert_input = '1' # Column input to sort by
    another_input = '1' # Input to sort in ascending or descending order
    the_table = select_table() # Get table input from usesr
    print("Sorting data from table " + the_table + ".\n")
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]


    print("Columns: " + ', '.join(colnames))

    print("SELECT * FROM " + the_table + " ORDER BY Column (ASC/DESC)")
 
    insert_input = input("Enter column name to sort by. => ")
    another_input = input("Enter 'ASC' to sort in ascending order, or 'DESC' to sort in descending order. => ")
    
    try:
        cursor.execute("SELECT * FROM " + the_table + " ORDER BY " + insert_input + " " + another_input + " LIMIT 15")
    except Exception as e:
        print("ERROR: " + e)
        exit()


    print("QUERY EXECUTED:\nSELECT * FROM " + the_table + " ORDER BY " + insert_input + " " + another_input)       

    print("RESULT: ")
    for row in cursor.fetchall():
        print(row)
    print("Limit of 15 entries displayed for readability\n")

# Function for #7 CLI Implementation: Joins
def joins_data():
    insert_input = '1' # Input for type of join operation
    key1 = '1' # Key input for first table
    key2 = '1' # Key input for second table
    the_table = select_table() # Get first table input from user
    the_table2 = select_table() # Get second table input from user
    print("Joining tables " + the_table + " and " + the_table2 + ".\n")
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]
    cursor.execute("Select * FROM " + the_table2 + " LIMIT 0")
    colnames2 = [desc[0] for desc in cursor.description]


    print(the_table + " Columns: " + ', '.join(colnames))
    print(the_table2 + " Columns: " + ', '.join(colnames2))

    print("SELECT * FROM " + the_table + " (join operation) " + the_table2 + " ON " + the_table + ".key = " + the_table2 + ".key\n")
 
    insert_input = input("Enter a join operation (e.g. join, natural join, left join, etc). => ")
    print(the_table + " Columns: " + ', '.join(colnames))
    key1 = input("Enter a key for " + the_table + " => ")
    print(the_table2 + " Columns: " + ', '.join(colnames2))
    key2 = input("Enter a key for " + the_table2 + " => ")

    try:
        cursor.execute("SELECT * FROM " + the_table + " " + insert_input + " " + the_table2 + " ON " + the_table + "." + key1 + " = " + the_table2 + "." + key2 + " LIMIT 15")
    except Exception as e:
        print("ERROR: " + e)
        exit()

    print("QUERY EXECUTED:\nSELECT * FROM " + the_table + " " + insert_input + " " + the_table2 + " ON " + the_table + "." + key1 + " = " + the_table2 + "." + key2)
        
    print("RESULT: ")
    for row in cursor.fetchall():
        print(row)
    print("Limit of 15 entries displayed for readability\n")

# Function for #8 CLI Implementation: Grouping
def grouping_data():
    insert_input = '1' # Column input to group by
    the_table = select_table() # Get table input from user
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]


    print(the_table + " Columns: " + ', '.join(colnames))

    print("SELECT (column), count(*) FROM " + the_table + " GROUP BY (column)\n")
 
    insert_input = input("Enter a column to group by => ")

    try:
        cursor.execute("SELECT " + insert_input + ", count(*) FROM " + the_table + " GROUP BY " + insert_input + " LIMIT 15")
    except Exception as e:
        print("ERROR: " + e)
        exit()

    print("QUERY EXECUTED:\nSELECT " + insert_input + ", count(*) FROM " + the_table + " GROUP BY " + insert_input)

    print("RESULT: ")
    for row in cursor.fetchall():
        print(row)
    print("Limit of 15 entries displayed for readability\n")

# Function for #9 CLI Implementation: Subqueries
def subquery_data():
    insert_input = '1' # Column input to look for in subquery
    another_input = '1' # A query input for the subquery
    the_table = select_table() # Get table input from user
    cursor.execute("Select * FROM " + the_table + " LIMIT 0")
    colnames = [desc[0] for desc in cursor.description]


    print(the_table + " Columns: " + ', '.join(colnames))

    print("SELECT * FROM " + the_table + " WHERE (column) in (subquery)\n")
 
    insert_input = input("Enter a column for (column) => ")

    another_input = input("Enter in an entire subquery. => ")

    try:
        cursor.execute("SELECT * FROM " + the_table + " WHERE " + insert_input + " in (" + another_input +  ") LIMIT 15")
    except Exception as e:
        print("ERROR: " + e)
        exit()

    print("QUERY EXECUTED:\nSELECT * FROM " + the_table + " WHERE " + insert_input + " in (" + another_input +  ")")
        
    print("RESULT: ")
    for row in cursor.fetchall():
        print(row)
    print("Limit of 15 entries displayed for readability\n")

# Function for #10 CLI Implementation: Transactions
def transaction_data():
    insert_input = '1' # Input for type of transaction. 
    print("Transactions begin automatically. What type of transaction would you like to do?")
    insert_input = input("Enter 'COMMIT' or 'ROLLBACK'. => ")
    if insert_input == 'COMMIT':
        try:
            connection.commit()
        except Exception as e:
            print("ERROR: " + e)
            exit()
        print("If there were any pending transactions, they have now been committed.")
    else:
        try:
            connection.rollback()
        except Exception as e:
            print("ERROR: " + e)
            exit()
        print("If there were any pending transactions, they have now been rolled back.")

# Function for #11 CLI Implementation: Error Handling
def error_handling():
    print("Error handling information:\n")
    print("This CLI is created with the psycopg2 library for Python. Errors are handled automatically with try ... except blocks!\n")






## MAIN PORTION OF THE PROJECT

shouldExit = 0

while shouldExit != 1:
    user_input = get_input()
    if user_input == '1':
        print("Insert Data")
        insert_data()  
    elif user_input == '2':
        print("Delete Data")
        delete_data()
    elif user_input == '3':
        print("Update Data")
        update_data()
    elif user_input == '4':
        print("Search Data")
        search_data()
    elif user_input == '5':
        print("Aggregate Functions")
        aggregate_functions()
    elif user_input == '6':
        print("Sorting")
        sorting_data()
    elif user_input == '7':
        print("Joins")
        joins_data()
    elif user_input == '8':
        print("Grouping")
        grouping_data()
    elif user_input == '9':
        print("Subqueries")
        subquery_data()
    elif user_input == '10':
        print("Transactions")
        transaction_data()
    elif user_input == '11':
        print("Error Handling")
        error_handling()
    else:
        shouldExit = 1
        print("Exiting.")


# Exited at this point, close connection with database server
connection.close()


