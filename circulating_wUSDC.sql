WITH raw_data AS (
    SELECT
        livequery.live.udf_api('https://stablecoins.llama.fi/stablecoincharts/Sui?stablecoin=1') AS response
),
exploded_data AS (
    SELECT
        VALUE AS nested_data
    FROM raw_data,
    TABLE(FLATTEN(INPUT => PARSE_JSON(response):data)) -- Ensure `:data` correctly references the array
),
final_output AS (
    SELECT
        -- Convert UNIX timestamp to UTC date
        TO_TIMESTAMP_NTZ(nested_data:"date"::STRING)::DATE AS utc_date,
        -- Extract peggedUSD from each JSON object
        nested_data:"totalBridgedToUSD"::OBJECT:"peggedUSD"::NUMBER AS total_bridged_to_usdc,
        nested_data:"totalCirculating"::OBJECT:"peggedUSD"::NUMBER AS total_circulating_usdc,
        nested_data:"totalCirculatingUSD"::OBJECT:"peggedUSD"::NUMBER AS total_circulating_usdc_value
    FROM exploded_data
)
SELECT * FROM final_output;
