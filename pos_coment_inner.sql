/****** Script for SelectTopNRows command from SSMS  ******/
SELECT * 
  FROM [PROFUTURO.DW].[dbo].[Informe.Facebook_comments]  c
  inner join [PROFUTURO.DW].[dbo].[Informe.Facebook_post]  p
  on  c.postid_id =p.cod_postid
  where convert(date,p.Fecha)>='2021-11-01' and  convert(date,p.Fecha)<='2021-11-30'

  ---------------------------------
  SELECT * 
  FROM [PROFUTURO.DW].[dbo].[Informe.Facebook_comments]  p
  
  where convert(date,p.Fecha)>='2021-11-01' and  convert(date,p.Fecha)<='2021-11-30'

  -------------------
