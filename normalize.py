import pandas as pd

class NormalizeTable:
    def __init__(self, df):
        self.df = df
    	

    def drop_dublices(self):
        self.df = self.df.drop_duplicates()


    def drop_nulls(self):
        self.df.dropna(inplace = True)

    
    def finall_data(self, foreign_df):
        unique_for_product = self.df.product_id.unique()
        unique_for_sales = foreign_df.product_id.unique()
        p_s = set(unique_for_sales) - set(unique_for_product)
        sales_ = set(unique_for_sales) - p_s
        return sales_



    def new_df(self, foreign_df):
        final_sales = []
        sales_ = self.finall_data(foreign_df)
        for index, row in self.df.iterrows():
            if row['product_id'] in sales_:
                final_sales.append(row)


        final_sales_df = pd.DataFrame(final_sales)
        return final_sales_df

    def cleared_column(self):
        splited = self.df["h1"].str.split(".", n = 1, expand = True)
        self.df["h1"] = splited[1]

        splited = self.df["h2"].str.split(".", n = 1, expand = True)
        self.df["h2"] = splited[1]

        splited = self.df["h3"].str.split(".", n = 1, expand = True)
        self.df["h3"] = splited[1]

        return self.df


