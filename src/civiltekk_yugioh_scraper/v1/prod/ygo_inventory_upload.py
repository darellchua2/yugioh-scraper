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


def deduplicate_inventory_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(
        subset=["region", "set_card_name_combined", "set_name",
                "set_card_code_updated", "rarity_name"],
        keep='last'
    )


def upload_inventory_main(filename="YGOInventoryV2.xlsx",
                          filename_to_upload="YGOInventoryV2.csv",
                          s3_bucket_name='yugioh-storage',
                          ygo_inventory_data_table="ygo_inventory_data",
                          dir="",
                          is_to_save_to_mysql: bool = False,
                          is_to_save_to_s3: bool = True
                          ) -> None:
    filepath = get_file_path(filename)
    ygo_inventory_export_path = get_file_path("YGOInventoryV2.xlsx")
    if not filepath:
        print(f"File {filename} not found.")
        return

    if filepath:
        df = pd.read_excel(filepath)

        # Deduplicate based on key inventory fields, keeping the last occurrence
        df = deduplicate_inventory_df(df)
        with pd.ExcelWriter(ygo_inventory_export_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="V2", index=False)
        if is_to_save_to_s3:
            save_df_to_s3(df, s3_bucket_name, dir, filename_to_upload)
            print(
                f"Uploading to S3 bucket: {s3_bucket_name}, directory: {dir}, filename: {filename_to_upload}")
        if is_to_save_to_mysql:
            save_df_to_mysql(
                df, table_name=ygo_inventory_data_table, if_exists="replace")
            print(f"Uploading to MySQL table: {ygo_inventory_data_table}")
            # Save the DataFrame to MySQL
            print(
                f"Saving DataFrame to MySQL table: {ygo_inventory_data_table}")


if __name__ == "__main__":
    upload_inventory_main()
