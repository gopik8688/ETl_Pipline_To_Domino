import os

from domino_flows.operators import transform

DATA_DIR = os.path.join(
    os.getcwd(),
    "domino_flows",
    "data",
)

if __name__ == "__main__":

    input_file = os.path.join(
        DATA_DIR,
        "extracted.parquet",
    )

    transform(input_file)