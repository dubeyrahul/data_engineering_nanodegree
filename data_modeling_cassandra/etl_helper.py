import csv
# Map containing column-name to data-type mapping used for inserting to cassandra
COLUMN_TYPE_MAP = {
    'itemInSession': int,
    'sessionId': int,
    'length': float,
    'userId': int,
}


def extract_data_from_row(line, columns_to_extract, column_to_index_map):
    """
    Given a row of csv, extract relevant columns and return as tuple
    :param List[str] line: Row of csv containing list of string values
    :param Tuple[str] columns_to_extract: Name of columns to extract from the csv row
    :param Dict[str, int] column_to_index_map: Map containing column_name to column_number mapping
    :return Tuple of extracted row values
    :rtype Tuple[Any]
    """
    row_data = []
    for col in columns_to_extract:
        # Extract value from row based on column-index
        value = f"{line[column_to_index_map[col]]}"
        # Cast value to the right data-type using COLUMN_TYPE_MAP
        row_data.append(COLUMN_TYPE_MAP.get(col, str)(value))
    return tuple(row_data)


def insert_data_in_table(session, filename, table_name, column_names):
    """
    Helper function to insert data into cassandra table from a csv file
    :param cassandra-session session: Cassandra session object
    :param str filename: csv file to read data from
    :param str table_name: name of the table to insert data in
    :param Tuple[str] column_names: Columns to extract from file and insert into the table
    :return Nothing, insert data into the table
    :rtype None
    """
    print(f"Inserting data into {table_name}...")
    column_placeholder = "%s, "*len(column_names)
    placeholder_string = f"({column_placeholder[:-2]})"
    columns_in_query = "(" + ', '.join(column_names) + ")"
    with open(filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        header_row = next(csvreader)
        column_to_index_map = {name:index for index,name in enumerate(header_row)}
        for line in csvreader:
            query = f"INSERT INTO {table_name} "
            query = query + f"{columns_in_query} VALUES {placeholder_string}"
            data_values = extract_data_from_row(line, column_names, column_to_index_map)
            session.execute(query, data_values)
    print(f"Finished inserting data into {table_name}")
