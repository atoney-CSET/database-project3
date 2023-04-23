import pandas as pd

class Output:
    def __init__(self, columns, rows):
        self.select_cols = columns
        self.rows = rows

    def printOutput(self):
        cols = [None]*len(self.select_cols)
        for cl in self.select_cols:
            i = self.select_cols.get(cl).get("index")
            name = self.select_cols.get(cl).get("name")
            if name is not None:
                cols[i] = name
            else:
                cols[i] = cl

        df = pd.DataFrame(self.rows, columns=cols)

        print(df)

