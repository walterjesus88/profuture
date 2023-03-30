/****** Script for SelectTopNRows command from SSMS  ******/

  update [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments]
  set  TextBlobSENTIMENT='Neutro'
  where TextBlobSENTIMENT='Negativo' and Nombre='Profuturo AFP'


  ------------
  update [PROFUTURO.DW].[dbo].[Informe.Facebook_message]
  set type='Profuturo'
  where from_name='Profuturo AFP'
  ---
   update [PROFUTURO.DW].[dbo].[Informe.Facebook_message]
  set type='Usuarios'
  where from_name<>'Profuturo AFP'