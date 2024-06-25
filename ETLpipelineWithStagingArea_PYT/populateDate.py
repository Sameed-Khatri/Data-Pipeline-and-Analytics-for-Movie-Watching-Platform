import mysql.connector
from datetime import datetime

# Connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="dwh"
)
cursor = mydb.cursor()

def get_date_input(prompt):
    while True:
        date_str = input(prompt)
        try:
            # Check if the input is in the correct format
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            print("Invalid date format. Please use 'YYYY-MM-DD'.")

def call_populate_date_table_from_terminal():
    # Inform the user about the correct input format
    print("Please provide the dates in the format 'YYYY-MM-DD'. For example: 2019-01-01.")

    # Prompt user for start and end dates
    start_date = get_date_input("Enter start date (YYYY-MM-DD): ")
    end_date = get_date_input("Enter end date (YYYY-MM-DD): ")

    try:
        # Call the stored procedure with user input dates
        cursor.callproc('PopulateDateTable', (start_date, end_date))

        # Commit changes
        mydb.commit()

        # Close the cursor and connection
        cursor.close()
        mydb.close()

        print("Date table successfully populated.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Run the function to prompt user input and execute the procedure
call_populate_date_table_from_terminal()


if __name__ == "__main__":
    call_populate_date_table_from_terminal()