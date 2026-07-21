import os

from domino_flows.operators import incremental_load

DATA_DIR = os.path.join(
    os.getcwd(),
    "domino_flows",
    "data",
)

if __name__ == "__main__":

    input_file = os.path.join(
        DATA_DIR,
        "transformed.parquet",
    )

    incremental_load(input_file)