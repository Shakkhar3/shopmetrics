SELECT customer_id, 'prénom manquant' AS probleme_prenom
FROM raw_customers
WHERE first_name IS NULL

union ALL

SELECT customer_id, 'email manquant' AS probleme_email
FROM raw_customers
WHERE email IS NULL

UNION ALL

select item_id, 'erreur de prix' AS probleme_prix
from raw_order_items
WHERE unit_price < 1