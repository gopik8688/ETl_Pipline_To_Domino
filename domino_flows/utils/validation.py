import pandera as pa
import pandas as pd

# Central schema used for transformed data. Add more schemas per stage as needed.
schema = pa.DataFrameSchema({
    'id': pa.Column(dtype=int),
    'value': pa.Column(dtype=float, nullable=False),
    'ts': pa.Column(dtype=pd.Timestamp)
})


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Validate DataFrame against the default schema. Raises on failure."""
    validated = schema.validate(df, lazy=True)
    print('Validation passed')
    return validated


def validate_stage(stage: str, df: pd.DataFrame) -> pd.DataFrame:
    """Choose a schema based on the pipeline stage and validate.

    `stage` examples: 'extract', 'transform', 'load'. Currently all stages
    use the same `schema` placeholder — extend this mapping for stage-specific
    rules.
    """
    # Map stages to schemas; extend when you add more stage-specific rules
    stage_map = {
        'transform': schema,
        'extract': schema,
        'load': schema,
    }
    s = stage_map.get(stage, schema)
    validated = s.validate(df, lazy=True)
    print(f'Validation passed for stage: {stage}')
    return validated
