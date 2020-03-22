import csv
column_type_map = {
    'itemInSession': int,
    'sessionId': int,
    'length': float,
    'userId': int,
}

def get_column_name_to_index_map(filename):
    with open(filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        for line in csvreader:
            column_values = line
            break
    return {name:index for index,name in enumerate(column_values)}

def get_column_name_to_index_map(filename):
    with open(filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        for line in csvreader:
            column_values = line
            break
    return {name:index for index,name in enumerate(column_values)}


def extract_data_from_row(line, columns_to_extract):
    row_data = []
    for col in columns_to_extract:
        row_data.append(get_column_from_line(line, col))
    return tuple(row_data)

def get_column_from_line(line, col_name):
    value = f"{line[column_to_index_map[col_name]]}"
    if col_name in column_type_map:
        value = column_type_map[col_name](value)
    return value

def insert_data_in_table(session, table_name, column_names):
    print(f"Inserting data into {table_name}...")
    file = 'event_datafile_new.csv'
    column_placeholder = "%s, "*len(column_names)
    placeholder_string = f"({column_placeholder[:-2]})"
    columns_in_query = "(" + ', '.join(column_names) + ")"
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = f"INSERT INTO {table_name} "
            query = query + f"{columns_in_query} VALUES {placeholder_string}"
            data_values = extract_data_from_row(line, column_names)
            session.execute(query, data_values)
    print(f"Finished inserting data into {table_name}")

column_to_index_map = get_column_name_to_index_map('event_datafile_new.csv')
