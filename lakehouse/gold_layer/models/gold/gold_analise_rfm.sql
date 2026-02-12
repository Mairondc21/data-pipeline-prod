WITH cte_base AS (
    SELECT
        sk_cliente,
        sk_data_pedido,
        sk_endereco,
        order_id,
        subtotal
    FROM
        {{ source('external_source','ft_vendas') }}
),
cte_dim_endereco AS (
    SELECT
        sk_endereco,
        cidade_envio,
        estado_envio
    FROM
        {{ source('external_source', 'dim_endereco_entrega') }}
),
cte_dim_cliente AS (
    SELECT
       sk_cliente,
       customer_id,
       cpf,
       nome,
       sobrenome
    FROM
        {{ source('external_source', 'dim_cliente') }} 
),
cte_dim_data AS (
    SELECT
        sk_date,
        data_completa
    FROM
        {{ source('external_source', 'dim_data') }}
),
join_base_cliente_endereco AS (
    SELECT
        bs.*,
        cidade_envio,
        estado_envio,
        customer_id,
        cpf,
        nome,
        sobrenome,
        data_completa
    FROM
        cte_base bs
    LEFT JOIN cte_dim_endereco ed on ed.sk_endereco = bs.sk_endereco
    LEFT JOIN cte_dim_cliente cl ON cl.sk_cliente = bs.sk_cliente
    LEFT JOIN cte_dim_data dt ON dt.sk_date = bs.sk_data_pedido
),
cte_agregacoes AS (
    SELECT
        customer_id,
        cpf,
        nome,
        sobrenome,
        cidade_envio,
        estado_envio,
        MIN(data_completa) AS data_primeira_compra,
        MAX(data_completa) AS data_ultima_compra,
        TODAY() - MAX(data_completa) AS dias_desde_ultima_compra,
        COUNT(order_id) AS qtd_pedidos,
        ROUND(SUM(subtotal),2) AS receita_total,
        ROUND((SUM(subtotal)) / COUNT(order_id), 2) AS ticket_medio
    FROM
        join_base_cliente_endereco
    GROUP BY
        customer_id,
        cpf,
        nome,
        sobrenome,
        cidade_envio,
        estado_envio

),
cte_classificacao AS (
    SELECT
        *,
        CASE
            WHEN dias_desde_ultima_compra BETWEEN 1 AND 90 THEN 5
            WHEN dias_desde_ultima_compra BETWEEN 91 AND 180 THEN 4
            WHEN dias_desde_ultima_compra BETWEEN 181 AND 365 THEN 3
            WHEN dias_desde_ultima_compra BETWEEN 366 AND 545 THEN 2
            ELSE 1
        END AS score_recency,
        CASE
            WHEN qtd_pedidos BETWEEN 1 AND 2 THEN 1
            WHEN qtd_pedidos BETWEEN 3 AND 5 THEN 2
            WHEN qtd_pedidos BETWEEN 6 AND 7 THEN 3
            WHEN qtd_pedidos BETWEEN 8 AND 9 THEN 4
            ELSE 5
        END AS score_frequency,
        CASE
            WHEN receita_total BETWEEN 1.00 AND 200.00 THEN 1
            WHEN receita_total BETWEEN 201.00 AND 1000.00 THEN 2
            WHEN receita_total BETWEEN 1001.00 AND 10000.00 THEN 3
            WHEN receita_total BETWEEN 10001.00 AND 35000.00 THEN 4
            ELSE 5
        END AS score_monetary
    FROM
        cte_agregacoes
),
cte_concat_score AS (
    SELECT
        *,
        CONCAT(score_recency,score_recency,score_monetary) AS score_rfm,
        CASE
            WHEN score_rfm IN(555, 554, 544, 545) THEN 'Campeões'
            WHEN score_rfm IN(543, 444, 435) THEN 'Clientes Leais'
            WHEN score_rfm IN(533, 534, 525) THEN 'Potencial Lealdade'
            WHEN CAST(score_rfm AS VARCHAR) LIKE '5%1' OR  CAST(score_rfm AS VARCHAR) LIKE '4%1' THEN 'Novos Clientes'
            WHEN score_rfm IN(244, 234, 334) THEN 'Em Risco'
            WHEN score_rfm IN(255, 155) THEN 'Não Pode Perder'
            WHEN score_rfm IN(111, 112, 121,224) THEN 'Perdidos'
            ELSE 'N/A'
        END AS segmento_cliente
    FROM
        cte_classificacao
)
SELECT
    *
FROM
    cte_concat_score