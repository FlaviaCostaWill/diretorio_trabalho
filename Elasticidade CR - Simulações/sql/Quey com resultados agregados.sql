select * from public.rentabilidade_cartoes_diego_camilo
limit 10


select * from cx_sandbox_zone.report_perdas_cr
where clientes_recorte_total > clientes_cx 
limit 5


select
mesref
, sum(ltv) as montante_ltv
, avg(ltv) as media_ltv
, approx_percentile(ltv, 0.5) as mediana_ltv
,  count(distinct l.cpf) as clientes_rentaveis_total
from public.rentabilidade_cartoes_diego_camilo l
left join growth_curated_zone.proposal_analysis pr on (pr.cpf = l.cpf)
where date_diff('month', cast(pr.dt_cfi_account as date), current_date) <= 12
group by 1
order by mesref




----------------------------------------------------------
--------- DADOS PARA PERDA DE ESTIMATIVA DE CR -----------
----------------------------------------------------------
with parametro as (
---------------- Queda de 10% de CR ---------------------
select 0.10 as queda_cr
)
, calculo_clientes as (
select 
	---- Clientes que deixam de ser atendidos  ------
round(clientes_cx * p.queda_cr, 0) as deixam_atendimento, 
round(clientes_cx * p.queda_cr  * perc_recorte, 0) as deixam_atendimento_recorte , 
round(clientes_cx * p.queda_cr  * perc_recorte * perc_churn_recorte * perc_recuperacao_churn, 0) as clientes_potencial_recuperacao
, *
from cx_sandbox_zone.report_perdas_cr
left join parametro as p on (1=1)
)
, calculo_ltv as (
	select *,
	--ltv por cliente  * clientes recuperados final (usando parametro de queda de cr)
	case when clientes_recorte_churn_recuperados > 0 then (LTV_Receita_Recuperado / clientes_recorte_churn_recuperados) * clientes_potencial_recuperacao else 0 end as ltv_receita_recuperado_final
	from calculo_clientes
)
, base_final as (
	select 
	dt_mes
	, hierarquia_1 
	, hierarquia_4
	, queda_cr
	, deixam_atendimento
	, perc_recorte
	, perc_churn_recorte
	, perc_recuperacao_churn
	, clientes_potencial_recuperacao
	, ltv_receita_recuperado_final
	-- Taxa geral de LTV por cliente que deixa de ser atendido
	, case when deixam_atendimento > 0 then ltv_receita_recuperado_final / deixam_atendimento else 0 end as ltv_incremental_cliente
	, case when media_ltv > 0 and deixam_atendimento > 0  then ltv_receita_recuperado_final / deixam_atendimento / media_ltv else 0 end as perc_ltv_cliente
	, case when montante_ltv  > 0 then ltv_receita_recuperado_final / montante_ltv else 0 end as perc_ltv_total
	from calculo_ltv
)
--select * from base_final --aberto por mes e hierarquia 4
select
--dt_mes,
hierarquia_1,
--incluir novas chaves de agregação
 sum(ltv_receita_recuperado_final) as ltv_receita_recuperado_final
, sum(ltv_incremental_cliente * deixam_atendimento) / sum(deixam_atendimento) as ltv_incremental_cliente
, sum(perc_ltv_cliente * deixam_atendimento) / sum(deixam_atendimento) as perc_ltv_cliente
, sum(perc_ltv_total) as perc_ltv_total
from base_final
group by 1
