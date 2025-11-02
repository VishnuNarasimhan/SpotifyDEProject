class reusable:
    def dropColumns(self, df, columns):
        return df.drop(*columns) # * unpacks the list of columns to be dropped