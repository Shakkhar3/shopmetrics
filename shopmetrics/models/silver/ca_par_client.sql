WITH commandes_completed AS (
    SELECT customer_id, order_id, order_date, total_amount
    FROM raw_orders
    WHERE status = 'completed'
),
ca_par_client AS (
    SELECT customer_id, SUM(total_amount) AS CA
    FROM commandes_completed
    GROUP BY customer_id
)
SELECT first_name, last_name, CA as total_depense
FROM ca_par_client
JOIN raw_customers ON ca_par_client.customer_id = raw_customers.customer_id
WHERE CA > 100