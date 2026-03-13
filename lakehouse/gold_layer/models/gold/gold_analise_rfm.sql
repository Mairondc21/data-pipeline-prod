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
    WHERE flag_atual = true
),
cte_dim_data AS (
    SELECT
        sk_date,
        data_completa
    FROM
        {{ source('external_source', 'dim_data') }}
),
join_vendas_data AS (
    SELECT
        bs.sk_cliente,
        bs.order_id,
        bs.subtotal,
        dt.data_completa,
        ed.cidade_envio,
        ed.estado_envio
    FROM cte_base bs
    INNER JOIN cte_dim_data dt ON bs.sk_data_pedido = dt.sk_date
    INNER JOIN cte_dim_endereco ed ON bs.sk_endereco = ed.sk_endereco
),
cte_agregacoes AS (
    SELECT
        cl.customer_id,
        cl.cpf,
        cl.nome,
        cl.sobrenome,
        vd.cidade_envio,
        vd.estado_envio,
        MIN(vd.data_completa) AS data_primeira_compra,
        MAX(vd.data_completa) AS data_ultima_compra,
        TODAY() - MAX(vd.data_completa) AS dias_desde_ultima_compra,
        COUNT(DISTINCT vd.order_id) AS qtd_pedidos,
        ROUND(SUM(subtotal),2) AS receita_total,
        ROUND((SUM(subtotal)) / COUNT(DISTINCT vd.order_id), 2) AS ticket_medio,
        TODAY() - MIN(vd.data_completa) AS tempo_como_cliente_dias
    FROM
        cte_dim_cliente cl
    INNER JOIN join_vendas_data vd ON cl.sk_cliente = vd.sk_cliente
    GROUP BY
        cl.customer_id,
        cl.cpf,
        cl.nome,
        cl.sobrenome,
        vd.cidade_envio,
        vd.estado_envio

),
cte_classificacao AS (
    SELECT
        *,
        CASE
            WHEN dias_desde_ultima_compra <= 30 THEN 5
            WHEN dias_desde_ultima_compra <= 90 THEN 4
            WHEN dias_desde_ultima_compra <= 180 THEN 3
            WHEN dias_desde_ultima_compra <= 365 THEN 2
            ELSE 1
        END AS score_recency,
        CASE
            WHEN qtd_pedidos >= 10 THEN 5
            WHEN qtd_pedidos >= 7 THEN 4
            WHEN qtd_pedidos >= 4 THEN 3
            WHEN qtd_pedidos >= 2 THEN 2
            ELSE 1
        END AS score_frequency,
        CASE
            WHEN receita_total >= 10000 THEN 5
            WHEN receita_total >= 5000 THEN 4
            WHEN receita_total >= 1000 THEN 3
            WHEN receita_total >= 500 THEN 2
            ELSE 1
        END AS score_monetary
    FROM
        cte_agregacoes
),
cte_concat_score AS (
    SELECT
        *,
        CONCAT(score_recency,score_frequency,score_monetary) AS score_rfm,
        CASE
            WHEN score_recency = 5 AND score_frequency = 5 AND score_monetary >= 4 THEN 'Campeões'
            WHEN score_recency = 5 AND score_frequency = 4 AND score_monetary >= 4 THEN 'Campeões'
            WHEN score_recency = 4 AND score_frequency = 5 AND score_monetary = 5 THEN 'Campeões'

            WHEN score_frequency >= 4 AND score_recency >= 3 THEN 'Clientes Leais'
            
            WHEN score_recency >= 4 AND score_monetary >= 3 AND score_frequency = 3 THEN 'Potencial Lealdade'
            
            WHEN score_recency >= 4 AND score_frequency = 1 THEN 'Novos Clientes'
            
            WHEN score_recency = 3 AND score_frequency >= 3 THEN 'Precisam Atenção'
    
            WHEN score_recency <= 2 AND score_frequency >= 3 AND score_monetary >= 3 THEN 'Em Risco'
            
            WHEN score_recency <= 2 AND score_monetary >= 4 THEN 'Não Pode Perder'

            WHEN score_recency = 2 AND score_frequency <= 2 THEN 'Hibernando'

            WHEN score_recency = 1 AND score_frequency <= 2 THEN 'Perdidos'
            ELSE 'Outros'
        END AS segmento_cliente,
        CASE
            WHEN dias_desde_ultima_compra <= 90 THEN 'Ativo'
            WHEN dias_desde_ultima_compra <= 365 THEN 'Em Risco'
            ELSE 'Inativo'
        END AS status_cliente
    FROM
        cte_classificacao
)
SELECT
    *
FROM
    cte_concat_score