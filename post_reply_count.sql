/****** Script for SelectTopNRows command from SSMS  ******/
SELECT *
  FROM [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments] r
   inner join [PROFUTURO.DW].[dbo].[Informe.Facebook_post]  p
   on substring(r.postid,0,18) = p.cod_postid
  where r.Nombre='Profuturo AFP' and convert(date,p.Fecha,112) >='20211101' and  convert(date,p.Fecha,112) <='20211130'

  ------------.
  SELECT *
  FROM [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments] 
  where --r.Nombre='Profuturo AFP' and 
  convert(date,Fecha,112) >='20211101' and  convert(date,Fecha,112) <='20211130'
