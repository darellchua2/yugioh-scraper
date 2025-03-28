import pandas as pd
import numpy as np

from ..utilities.misc_utilities import get_file_path


def hitpay_main():
    filename = "Orders-Export-2023-April-16-0629.csv"
    export_filepath = "income_output.csv"
    orders_import_path = get_file_path(filename)
    orders_export_path = get_file_path(export_filepath)
    df: pd.DataFrame = pd.read_csv(orders_import_path)

    df_unique: pd.DataFrame = df.drop_duplicates(
        keep='last', subset=['Order ID'])
    df_unique['Order Date'] = pd.to_datetime(df_unique['Order Date'])
    df_unique['Order Date Updated'] = df_unique['Order Date'].dt.date
    df_unique = df_unique[df_unique['Order Status'] == 'wc-completed']

    df_unique['Category'] = 'Income'
    df_unique['Subcategory'] = np.nan
    df_unique['Order ID Updated'] = "#" + df_unique['Order ID'].astype(str)
    df_unique['Hitpay Fee'] = df_unique['HitPay_fees']
    df_unique['Stripe Net'] = df_unique['_stripe_net']
    df_unique['Ref No'] = np.nan
    df_unique['Payer'] = df_unique['Billing First Name'] + \
        " " + df_unique['Billing Last Name']
    df_unique['Status'] = 'Cleared'
    df_unique['picture'] = np.nan
    df_unique['Account'] = 'YGO'

    df_unique = df_unique[['Order Date Updated', 'Order Total',
                           'Category', 'Subcategory',
                           'Payment Method Title', 'Order ID Updated',
                           'Hitpay Fee', 'Stripe Net',
                           'Ref No', 'Payer', 'Status', 'picture', 'Account'
                           ]]
    print(df_unique.dtypes)
    df_unique.to_csv(orders_export_path, index=False)

    print(df)


if __name__ == "__main__":
    hitpay_main()
