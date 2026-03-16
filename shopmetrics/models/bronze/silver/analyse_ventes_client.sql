SELECT customer_id, order_id, order_date, total_amount,
    SUM(total_amount) over (partition by customer_id order by order_date) AS cumul_achat,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rang,
    LAG(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS commande_precedente,
FROM raw_orders
WHERE status = 'completed'