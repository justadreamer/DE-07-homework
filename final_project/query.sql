-- В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду вересня?

WITH t AS (
    SELECT upe.state,
           COUNT(s._id) COUNT
    FROM `de-07-376021.silver.sales` AS s
        INNER JOIN `de-07-376021.gold.user_profiles_enriched` AS upe
    ON s.client_id = upe.client_id
    WHERE s.product_name='TV'
      AND upe.birth_date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 YEAR) AND DATE_SUB(CURRENT_DATE, INTERVAL 20 YEAR)
      AND purchase_date BETWEEN DATE ('2022-09-01') AND DATE ('2022-09-10')
    GROUP BY upe.state
    ORDER BY COUNT DESC
    LIMIT 1
)
SELECT state
FROM t;

-- Iowa
