WITH tb_pedido as (
  SELECT DISTINCT
        t1.idPedido,
        t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '{date}'

  AND t1.dtPedido >= add_months('{date}', -6)
  AND t2.idVendedor IS NOT NULL
),

tb_join as (

  SELECT t1.*,
          t2.vlNota

  FROM tb_pedido as t1

  LEFT JOIN silver.olist.avaliacao_pedido AS t2
  ON t1.idPedido = t2.idPedido
),

tb_summary as (
  SELECT idVendedor,
          avg(vlNota) as avgNota,
          percentile(vlnota, 0.5) as medianNota,
          min(vlNota) as minNota,
          max(vlNota) as maxNota,
          count(vlNota) / count(idPedido) as pctAvaliacao

  FROM tb_join

  GROUP BY idVendedor
)


SELECT '{date}' as dtReference,
        *

FROM tb_summary