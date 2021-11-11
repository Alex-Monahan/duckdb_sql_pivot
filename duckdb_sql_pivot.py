

# Eventually a back end in Python is a good idea. I'm going to start with JS though.


import duckdb
import pandas as pd

def main():
    print('Woot!')

    try:
        pass
    finally:
        pass

class sql_pivot:
    # Pass in a DuckDB connection so that this can be chained with other operations
    # Pass in a table or view name, then rows, columns, values (expressions), filters
    # Need special expression/keyword for the "sigma" like in other pivots (where do the values go if there are multiple - rows or columns?)

    def __init__(self, conn, table_or_view, rows=None, columns=None, values=None, filters=None):
        pass

class pivot_to_grid:

    def __init__(self, table_or_view, rows=None, columns=None, values=None, filters=None):
        pass

if __name__ == '__main__':
    main()