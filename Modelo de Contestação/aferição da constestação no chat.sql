SELECT 
cast(c.dt_chat as date) as data_chat
, c.cpf_customer
, max(nr_tpr) as nr_tpr
, max(nr_tma) as nr_tma
, max(nr_tmat) as nr_tmat
, max(nm_tag_hierarquia_1) as hierarquia_1
FROM cx_curated_zone.cx_contact_rate c
LEFT JOIN cx_curated_zone.helpshift_issues AS issues_macro ON c.id_issue = issues_macro.id_issue
LEFT JOIN cx_curated_zone.helpshift_tag_niveis AS helpshift_tag ON c.id_issue = helpshift_tag.id_issue
LEFT JOIN processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags ON trim(lower(n3)) = trim(lower(nm_tag_n3))
WHERE dt_chat >= to_date('2023-05-30','yyyy-mm-dd') and dt_chat < to_date('2023-07-06','yyyy-mm-dd')
AND c.id_issue is not null and issues_macro.nr_tmat > 90 
and nm_tag_hierarquia_4 in ('Não reconhecimento de compra no crédito','Resultado análise de fraude','Problemas com a compra') 
GROUP BY 1,2