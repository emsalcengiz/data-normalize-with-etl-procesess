import pandas as pd
from datetime import timedelta, datetime

class AverageSales:
    def __init__(self, sales, inventory):
        self.sales = sales
        self.inventory = inventory


    def merge(self):
        self.merged_df = pd.merge(self.sales, self.inventory, on=['store_id', 'product_id', 'date'])


    def convert_datetime(self):
        self.merged_df['date'] = self.merged_df['date'].astype('datetime64[ns]')


    def before_days_calculation(self):
        datetime_object = datetime.strptime('2017-05-22', '%Y-%m-%d')
        self.before_day = datetime_object - timedelta(days=30)


    def average(self):     
        positive_inventory = self.merged_df.query(f"inventory > 0 & date > '{self.before_day.strftime('%Y-%m-%d')}'")
        self.positive_inventory = positive_inventory.groupby(['store_id','product_id']).agg(average=('unit','mean')).reset_index()


    def to_csv(self):
        self.positive_inventory.to_csv("datasets/last_30_days_sales_average.csv", index=True)





