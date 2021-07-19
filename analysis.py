import pandas as pd
from datetime import timedelta, datetime


class Analysis:
    def __init__(self, sales, inventory, product):
        self.sales = sales
        self.inventory = inventory
        self.product = product

        merged_sales_inventory = pd.merge(self.sales, self.inventory, on=['store_id', 'product_id', 'date'])
        merged_df = pd.merge(merged_sales_inventory, self.product, on=['product_id'])
        merged_df['date'] = merged_df['date'].astype('datetime64[ns]')

        self.merged_df = merged_df
        self.datetime_object = datetime.strptime('2017-05-22', '%Y-%m-%d')

        self.final_df = None


    def last_7_days_sales(self):
        before_day = self.datetime_object - timedelta(days=7)
        before_day_df = self.merged_df.query(f"date >= '{before_day.strftime('%Y-%m-%d')}'")
        before_day_df = before_day_df.groupby(['store_id', 'product_id']).agg(last_7_days_sales=('unit', 'sum')).reset_index()
        self.final_df = before_day_df
        



    def last_30_days_sales(self):
        before_day = self.datetime_object - timedelta(days=30)
        before_day_df = self.merged_df.query(f"date >= '{before_day.strftime('%Y-%m-%d')}'")
        before_day_df = before_day_df.groupby(['store_id', 'product_id']).agg(last_30_days_sales=('unit', 'sum')).reset_index()
        self.final_df = pd.merge(self.final_df, before_day_df, on = ['store_id', 'product_id'])
        
    

    def last_year_following_7days_sales(self):
        datetime_object = datetime.strptime('2016-05-22', '%Y-%m-%d')
        before_day = datetime_object - timedelta(days=7)
        before_day_df = self.merged_df.query(f"date >= '{before_day.strftime('%Y-%m-%d')}'")
        before_day_df = before_day_df.groupby(['store_id', 'product_id']).agg(last_year_following_7days_sales=('unit', 'sum')).reset_index()
        self.final_df = pd.merge(self.final_df, before_day_df, on = ['store_id', 'product_id'])
        


    def most_recent_inventory_value(self):
        inventory_value = self.merged_df.groupby(['store_id', 'product_id']).agg(most_recent_inventory_value=('inventory', 'sum')).reset_index()
        self.final_df = pd.merge(self.final_df, inventory_value, on = ['store_id', 'product_id'])
        


    def last30_days_positive_inventory(self):
        before_day_df = self.merged_df.query("inventory > 0")
        positive_inventory = before_day_df.groupby(['store_id', 'product_id']).agg(last30_days_positive_inventory=('date','count')).reset_index()
        self.final_df = pd.merge(self.final_df, positive_inventory, on = ['store_id', 'product_id'])
        


    def merge_product(self):
        self.final_df = pd.merge(self.product, self.final_df, on = ['product_id'])
        self.final_df = self.final_df.loc[:, ~self.final_df.columns.str.contains('^Unnamed')]
        


    def to_csv(self):
        self.final_df = self.final_df.reindex(columns=['store_id','product_id','h1','h2','h3','last_7_days_sales','last_30_days_sales', 'last_year_following_7days_sales', 'most_recent_inventory_value','last30_days_positive_inventory','average'])
        self.final_df = self.final_df.sort_values(by=['store_id', 'product_id'])
        self.final_df.to_csv('datasets/analysis.csv', index= False)
        


    def average(self):
        before_day = self.datetime_object - timedelta(days=30)     
        positive_inventory = self.merged_df.query(f"inventory > 0 & date > '{before_day.strftime('%Y-%m-%d')}'")
        positive_inventory = positive_inventory.groupby(['store_id','product_id']).agg(average=('unit','mean')).reset_index()
        self.final_df = pd.merge(positive_inventory, self.final_df, on = ['store_id','product_id'])
        







