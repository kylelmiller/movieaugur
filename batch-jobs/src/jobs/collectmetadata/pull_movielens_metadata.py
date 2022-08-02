import argparse

def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--asset_metadata_location",
        type=local_location,
        help="s3 location where the additional asset metadata is located.",
        required=True,
    )

def main():
    pass

if __name__ == "__main__":
    main()