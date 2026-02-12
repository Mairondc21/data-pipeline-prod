WITH cte_base AS (
    SELECT
        sk_produto,
        sk_data_pedido,
        sk_cliente,
        order_id,
        quantidade,
        desconto,
        frete,
        subtotal
    FROM
        {{ source('external_source','ft_vendas') }}
),
cte_dim_data AS (
    SELECT
        sk_date,
        ano,
        mes,
        nome_mes,
        trimestre
    FROM
        {{ source('external_source','dim_data') }}
),
cte_dim_cliente AS (
    SELECT
        sk_cliente,
        customer_id
    FROM
        {{ source('external_source','dim_cliente') }}
),
join_base_data_cliente AS (
    SELECT
        bs.*,
        ano,
        mes,
        nome_mes,
        trimestre,
        customer_id
    FROM
        cte_base bs
    LEFT JOIN cte_dim_data dt ON dt.sk_date = bs.sk_data_pedido
    LEFT JOIN cte_dim_cliente cl ON cl.sk_cliente = bs.sk_cliente
),
cte_agregacoes AS (
    SELECT
        ano,
        mes,
        nome_mes,
        trimestre,
        COUNT(DISTINCT order_id) AS qtd_pedidos,
        COUNT(DISTINCT customer_id) AS qtd_clientes_unicos,
        SUM(quantidade) AS total_itens_vendidos,
        ROUND(SUM(subtotal),2) AS receita_bruta,
        ROUND(SUM(desconto),2) AS total_descontos,
        ROUND(SUM(frete),2) AS total_frete,
    FROM
        join_base_data_cliente
    GROUP BY
        ano,
        mes,
        nome_mes,
        trimestre
),
cte_final AS (
    SELECT
        *,
        ROUND(receita_bruta - total_descontos,2) AS receita_liquida,
        ROUND(receita_bruta / qtd_pedidos, 2) AS ticket_medio,
        ROUND(total_itens_vendidos / qtd_pedidos, 2) AS itens_por_pedido
    FROM
        cte_agregacoes
)
SELECT
    *
FROM
    cte_final