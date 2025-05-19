import pandas as pd
from ..utilities.aws_utilities import save_df_to_s3, get_engine_for_tekkx_scalable_db
from ..utilities.misc_utilities import get_file_path


def save_df_to_mysql(df: pd.DataFrame, table_name: str, if_exists="replace") -> None:
    _, engine = get_engine_for_tekkx_scalable_db(
        db_name="yugioh_data")  # or your actual DB
    try:
        df.to_sql(table_name, con=engine, index=False,
                  if_exists=if_exists, method='multi')  # type: ignore
        print(f"✅ Successfully uploaded to MySQL table: {table_name}")
    except Exception as e:
        print(f"❌ Error uploading to MySQL: {e}")


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
        # save_df_to_s3(df, s3_bucket_name, dir, filename_to_upload)
        save_df_to_mysql(df, table_name=ygo_inventory_data_table,
                         if_exists="replace")


if __name__ == "__main__":
    upload_inventory_main()
