import pandas as pd
from ..utilities.aws_utilities import save_df_to_s3, save_df_to_mysql
from ..utilities.misc_utilities import get_file_path


def upload_inventory_csv(filepath: str,
                         filename_to_upload: str,
                         s3_bucket_name: str,
                         dir="") -> None:
    # Open a file in binary mode ('rb') and read its contents into a BytesIO object
    df = pd.read_csv(filepath)
    print(df)

    save_df_to_s3(df, s3_bucket_name, dir, filename_to_upload)


def upload_inventory_main(filename="YGOInventoryV2.xlsx",
                          filename_to_upload="YGOInventoryV2.csv",
                          s3_bucket_name='yugioh-storage',
                          ygo_inventory_data_table="ygo_inventory_data",
                          dir=""
                          ) -> None:
    filepath = get_file_path(filename)
    if filepath:
        df = pd.read_excel(filepath)
        # filtered_df = df[
        #     df['post_title'].isna() |
        #     (df['post_title'] == '')
        #     # df['post_title'].str.endswith(" | Japanese", na=False)
        # ]

        save_df_to_s3(df, s3_bucket_name, dir, filename_to_upload)
        save_df_to_mysql(df, table_name=ygo_inventory_data_table,
                         if_exists="replace")
        # df.to_csv("sample.csv", index=False)


if __name__ == "__main__":
    upload_inventory_main()
