SELECT 
  ROUND((input_value - output_value)/ size, 0) AS fees_per_byte,
  COUNT(*) AS txn_cnt
FROM
  `bigquery-public-data.crypto_bitcoin.transactions`
WHERE TRUE
  AND block_timestamp >= '2018-01-01' 
  AND is_coinbase IS FALSE
GROUP BY 1