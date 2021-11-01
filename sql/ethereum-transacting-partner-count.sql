SELECT
	txn_count,
	COUNT(txn_count) AS num_addresses
FROM
(
	SELECT 
	  ARRAY_TO_STRING(inputs.addresses, '') AS addresses,
	  COUNT(DISTINCT `hash`) AS txn_count
	FROM `bigquery-public-data.crypto_bitcoin.transactions` AS txns
	CROSS JOIN UNNEST(txns.inputs) AS inputs
	GROUP BY addresses
)
GROUP BY txn_count
ORDER BY txn_count ASC