WITH tb_pedido_item AS (

  SELECT t2.*,
          t1.dtPedido

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL
),

tb_summary as (
  SELECT idVendedor,
        count(distinct idPedido) AS qtdPedidos,
        count(distinct date(dtPedido)) AS qtdDias,
        count(idProduto) AS qtdItens,
        min(datediff('2018-01-01', dtPedido)) AS qtdRecencia,
        sum(vlPreco) / count(distinct idPedido) AS avgTicket,
        avg(vlPreco) AS avgValorProduto,
        max(vlPreco) AS maxValorProduto,
        min(vlPreco) AS minValorProduto,
        count(idProduto) / count(distinct idPedido) AS avgProdutoPedido
      
  FROM tb_pedido_item
  
  GROUP BY idVendedor
),

tb_pedido_summary AS (
  SELECT idVendedor,
          idPedido,
          sum(vlPreco) as vlPreco

  FROM tb_pedido_item

  GROUP BY idVendedor,
            idPedido
),

tb_min_max AS (

  SELECT idVendedor,
          min(vlPreco) AS minVlPreco,
          max(vlPreco) AS maxVlPreco

  FROM tb_pedido_summary

  GROUP BY idVendedor
),

tb_life AS (
  SELECT t2.idVendedor,
          sum(vlPreco) AS LTV,
          max(datediff('2018-01-01', dtPedido)) AS qtdDiasBase

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t2.idVendedor IS NOT NULL
  
  GROUP BY t2.idVendedor
),

tb_dtpedido AS (

  SELECT DISTINCT idVendedor,
          date(dtPedido) as dtPedido

  FROM tb_pedido_item

  ORDER BY 1.2
),

tb_lag AS (

  SELECT *,
          LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1

  FROM tb_dtpedido
),

tb_intervalo AS (
  SELECT idVendedor,
          avg(datediff(dtPedido, lag1)) as avgIntervalVendas

  FROM TB_LAG

  GROUP BY idVendedor
)



SELECT '2018-01-01' as dtReference,
        t1.*,
        t2.minVlPreco,
        t2.maxVlPreco,
        t3.LTV,
        t3.qtdDiasBase,
        t4.avgIntervalVendas

FROM tb_summary AS t1

LEFT JOIN tb_min_max as t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN tb_life as t3
ON t1.idVendedor = t3.idVendedor

LEFT JOIN tb_intervalo as t4
ON t1.idVendedor = t4.idVendedor