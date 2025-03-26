from io import BytesIO
import pandas as pd
from ..utilities.aws_utilities import save_df_to_s3
from ..utilities.misc_utilities import get_file_path


def upload_inventory(filepath: str,
                     sheet_name: str,
                     filename_to_upload: str,
                     s3_bucket_name: str,
                     dir=""):

    df = pd.read_excel(filepath)
    save_df_to_s3(df, s3_bucket_name, dir, filename_to_upload)


def upload_inventory_csv(filepath: str,
                         sheet_name: str,
                         filename_to_upload: str,
                         s3_bucket_name: str,
                         dir=""):
    # Open a file in binary mode ('rb') and read its contents into a BytesIO object
    df = pd.read_csv(filepath)
    print(df)

    save_df_to_s3(df, s3_bucket_name, dir, filename_to_upload)


def main():
    filename = "YGOInventoryV2.xlsx"
    filepath = get_file_path(filename)
    print("filepath: ", filepath)
    sheet_name = "V2"
    filename_to_upload = "YGOInventoryV2.csv"
    s3_bucket_name = 'yugioh-storage'
    if filepath:
        upload_inventory(filepath=filepath,
                         sheet_name=sheet_name,
                         filename_to_upload=filename_to_upload,
                         s3_bucket_name=s3_bucket_name
                         )


if __name__ == "__main__":
    main()
