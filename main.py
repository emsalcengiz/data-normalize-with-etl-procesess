import pandas as pd
from datetime import timedelta, datetime
from normalize import NormalizeTable
from averageSales import AverageSales
from analysis import Analysis



if __name__ == "__main__":
    product = pd.read_csv("datasets/ptable.csv")
    sales = pd.read_csv("datasets/sales.csv")
    inventory = pd.read_csv("datasets/inventorydata.csv")


    normalized_sales = NormalizeTable(sales)
    normalized_sales.drop_dublices()
    normalized_sales.drop_nulls()
    sales = normalized_sales.new_df(product)
    sales.to_csv('datasets/normalized_sales.csv')

    normalized_sales = NormalizeTable(inventory)
    normalized_sales.drop_dublices()
    normalized_sales.drop_nulls()
    inventory = normalized_sales.new_df(product)
    inventory.to_csv('datasets/normalized_inventory.csv')

    normalized_sales = NormalizeTable(product)
    normalized_sales.drop_dublices()
    normalized_sales.drop_nulls()
    ptable = normalized_sales.cleared_column()
    ptable.to_csv('datasets/normalized_ptable.csv')


    sales = pd.read_csv("datasets/normalized_sales.csv")
    inventory = pd.read_csv("datasets/normalized_inventory.csv")
    product = pd.read_csv("datasets/normalized_ptable.csv")

    averageSales = AverageSales(sales, inventory)
    averageSales.merge()
    averageSales.convert_datetime()
    averageSales.before_days_calculation()
    averageSales.average()
    averageSales.to_csv()


    analysis = Analysis(sales, inventory, product)
    analysis.last_7_days_sales()
    analysis.last_30_days_sales()
    analysis.last_year_following_7days_sales()
    analysis.most_recent_inventory_value()
    analysis.last30_days_positive_inventory()
    analysis.average()
    analysis.merge_product()
    analysis.to_csv()


