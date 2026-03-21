select 
    DATE_TRUNC('month', order_date) as mois,
    count(order_id) as nb_commande,
    SUM (total_amount) as ca_total,
    ROUND(AVG(total_amount), 2) as panier_moyen,
    COUNT(DISTINCT customer_id) as nb_client,
FROM raw_orders
WHERE status = 'completed'
GROUP BY 1