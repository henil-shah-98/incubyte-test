import sqlite3

def read_input_file(input_file_path):
	"""
	Reads input file and store rowsas string in a list.

	Args:
		input_file_path: Location of input file.
	Returns:
		raw_data: List of rows in string data type.

	"""
	raw_data = list()

	with open(input_file_path, "r") as fp:
		for line in fp:
			raw_data.append(line)
	
	return raw_data

def format_input_raw_data(raw_data):
	"""
	Split data based on delimiter and store data in form 
	of list for dictionary of rows.

	Args:
		raw_data: List of rows in string data type.
	Returns:
		data_dict: List of dictionaries of rows.

	"""
	header = None
	data = list()
	data_dict = {"data" : list()}

	for line in raw_data:
		if line[:3] == "|H|":
			header = line[3:].split("|")
		elif line[:3] == "|D|":
			data.append(line[3:].split("|"))
		else:
			continue
	
	for row in data:
		l = len(row)
		row_dict = dict()
		
		for i in range(l):
			row_dict[header[i]] = row[i]
		
		row_dict["Postal_Code"] = int(row_dict["Postal_Code"])
		data_dict["data"].append(row_dict)
	
	return data_dict

def create_or_connect_database(db_path):
	"""
	Create or connect to database present in location.

	Args:
		db_path: Location of database file.
	Returns:
		con: SQLite connection.
		cur: Cursor to SQLite connection.

	"""
	con = sqlite3.connect(db_path)
	cur = con.cursor()
	
	return con, cur

def create_country_table(cur, country):
	"""
	Create table based on customer country.

	Args:
		cur: Cursor to SQLite connection.
		country: Name of country for which table is created.

	"""
	query = """ 
	CREATE TABLE IF NOT EXISTS table_{} ( 
	Customer_Name TEXT NOT NULL, 
	Customer_ID TEXT PRIMARY KEY, 
	Open_Date TEXT NOT NULL, 
	Last_Consulted_Date TEXT, 
	Vaccination_ID TEXT, 
	Dr_Name TEXT, 
	State TEXT, 
	Country TEXT, 
	Postal_Code INTEGER, 
	DOB TEXT, 
	Is_Active TEXT 
	 ) 
	""".format(country)
	
	cur.execute(query)

def insert_into_country_table(cur, row_dict):
	"""
	Insert row to its country table.

	Args:
		cur: Cursor to SQLite connection.
		row_dict: dictionary of a row.

	"""
	header = list()
	data = list()
	
	for k, v in row_dict.items():
		header.append(k)
		data.append(v)
	
	query = """ 
	INSERT INTO table_{} 
	({}) 
	VALUES (?,?,?,?,?,?,?,?,?,?,?) 
	""".format(row_dict["Country"], ",".join(header))
	
	cur.execute(query, data)

def print_all_tables(cur, countries):
	"""
	Print all country tables created.

	Args:
		cur: Cursor to SQLite connection.
		countries: Set of countries present in data

	"""
	query = """ 
	SELECT * FROM table_{}
	"""
	
	for country in countries:
		cur.execute(query.format(country))
		data = cur.fetchall()
		
		print("\ntable_{}\n".format(country))
		
		for row in data:
			print("\t".join([str(i) for i in row]))
		
		print()

if __name__ == '__main__':

	# Read input
	raw_data = read_input_file("./input.txt")

	# Format input to dictionary
	data_dict = format_input_raw_data(raw_data)

	# Connection and cursor
	con, cur = create_or_connect_database("./etl.db")

	# Create table and insert rows to tables
	countries = set()
	for row_dict in data_dict["data"]:
		countries.add(row_dict["Country"])
		create_country_table(cur, row_dict["Country"])
		insert_into_country_table(cur, row_dict)
	
	#Print all tables created
	print_all_tables(cur, countries)

	# Commit and close connection
	con.commit()
	con.close()