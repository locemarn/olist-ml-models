-- Databricks notebook source
WITH tb_pedido as (

  SELECT t1.idPedido,
          t2.idVendedor,
          t1.descSituacao,
          t1.dtPedido,
          t1.dtAprovado,
          t1.dtEntregue,
          t1.dtEstimativaEntrega,
          sum(vlFrete) as totalFrete

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL

  GROUP BY t1.idPedido,
            t2.idVendedor,
            t1.descSituacao,
            t1.dtPedido,
            t1.dtAprovado,
            t1.dtEntregue,
            t1.dtEstimativaEntrega
)


SELECT '2018-01-01' AS dtReference,
        idVendedor,

        count(
          distinct case when descSituacao = 'delivered' 
            AND date(coalesce(dtEntregue, '2018-01-01')) > date(dtEstimativaEntrega) 
          THEN idPedido END
        ) / count(
          distinct case when descSituacao = 'delivered'
          then idPedido end
        ) as pctPedidoAtraso,
        
        count(distinct case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) AS pctPedidoCancelado,
        
        avg(totalFrete) as avgFrete,
        percentile(totalFrete, 0.5) as medianFrete,
        max(totalFrete) as maxFrete,
        min(totalFrete) as minFrete,
        
        avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtAprovado)) as qtdDiasAprovadoEntrega,
        avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtPedido)) as qtdDiasPedidoEntrega,
        avg(datediff(dtEstimativaEntrega, coalesce(dtEntregue, '2018-01-01'))) as qtdeDiasEmtregaPromessa
        
FROM tb_pedido

GROUP BY idVendedor



-- COMMAND ----------


