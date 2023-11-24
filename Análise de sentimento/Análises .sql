select * from cx_sandbox_zone.analise_sentimento_teste
order by rand()
limit 100


select * from cx_curated_zone.hms limit 5

select distinct mes_referencia  from cx_curated_zone.hms


select 
--mes_chat
 case 
	when hms  <= 60 then '1. Ate 60%'
	when hms  <= 75 then '2. > 60-75%'
	when hms  <= 80 then '3. > 75-80%'
	else '4. Acima de 80%'
end as fx_hms
, sentimento_predito
, count(distinct cpf_customer) as clientes
from  cx_sandbox_zone.analise_sentimento_teste s 
left join cx_curated_zone.hms h on (s.cpf_customer = cast(h.cpf as double) 
				and substring(s.mes_chat,1,4) = substring(h.mes_referencia, 1, 4) 
				and substring(s.mes_chat,6,2) = substring(h.mes_referencia, 5, 2))
where cast(mes_chat as date) >= to_date('2023-08-01', 'yyyy-mm-dd')
group by 1, 2




select sentimento_predito
, count(distinct s.cpf_customer) as clientes 
, avg(case when ds_lealdade_nps = 'promotor' then 1 else 0 end) as cont_promotor
, avg(case when ds_lealdade_nps = 'neutro' then 1 else 0 end) as cont_neutro
, avg(case when ds_lealdade_nps = 'detrator' then 1 else 0 end) as cont_detrator
from cx_sandbox_zone.analise_sentimento_teste s 
left join cx_curated_zone.indecx_nps n on (cast(n.cpf as double) = s.cpf_customer)
group by 1


select * from cx_sandbox_zone.analise_sentimento_teste
limit 5

select tipo_msg_predito
, count(distinct s.cpf_customer) as clientes 
, avg(case when ds_lealdade_nps = 'promotor' then 1 else 0 end) as cont_promotor
, avg(case when ds_lealdade_nps = 'neutro' then 1 else 0 end) as cont_neutro
, avg(case when ds_lealdade_nps = 'detrator' then 1 else 0 end) as cont_detrator
from cx_sandbox_zone.analise_sentimento_teste s 
left join cx_curated_zone.indecx_nps n on (cast(n.cpf as double) = s.cpf_customer)
group by 1


select * from 
cx_curated_zone.segmentacao_ltv
limit 5


select * from cx_sandbox_zone.report_perdas_cr
limit 5




select max(mesref) from public.rentabilidade_cartoes_diego_camilo
where cast(concat(mesref, '-01') as date) >= to_date('2023-06-01', 'yyyy-mm-dd')


select * from public.rentabilidade_cartoes_diego_camilo
limit 5


describe cx_curated_zone.segmentacao_ltv

