------------------------------------------- a cne
drop table if exists sde.cne_mesas;
CREATE TABLE sde.cne_mesas as 
SELECT centro, 
       sum(case when  timestamprecepcion     IS NOT NULL THEN 1 ELSE 0 END) as recibidas,
       sum(case when  timestampinstalacion   IS NOT NULL THEN 1 ELSE 0 END) as instaladas,
       sum(case when  timestampconstitucion  IS NOT NULL THEN 1 ELSE 0 END) as constituidas,
       sum(case when  timestampapertura      IS NOT NULL THEN 1 ELSE 0 END) as aperturadas,
       sum(case when  timestampcierre        IS NOT NULL THEN 1 ELSE 0 END) as cerradas,
       sum(case when  timestamptransmision   IS NOT NULL THEN 1 ELSE 0 END) as transmitidas,
       COUNT(centro) AS mesasxcentro
  FROM sde.cne101920 ---> cambio tabla <-----
group by centro
order by centro;
---------------------------------------------  a proceso de mesas
UPDATE sde.cne_proceso
SET    mesasrecibidas    = cne_mesas.recibidas,
       mesasinstaladas   = cne_mesas.instaladas,
       mesasconstituidas = cne_mesas.constituidas,
       mesasaperturadas  = cne_mesas.aperturadas,
       mesascerradas     = cne_mesas.cerradas,
       mesastransmitidas = cne_mesas.transmitidas
FROM   sde.cne_mesas
WHERE  cne_proceso.codigocv = cne_mesas.centro;

---------------------------------------------  porcentaje

drop table if exists sde.cne_porcentaje;
CREATE TABLE sde.cne_porcentaje as 
SELECT centro, mesasxcentro,

        (recibidas      *100.00 / mesasxcentro) as porcentaje_recibidas,
        (instaladas     *100.00 / mesasxcentro) as porcentaje_instaladas,
        (constituidas   *100.00 / mesasxcentro) as porcentaje_constituidas,
        (aperturadas    *100.00 / mesasxcentro) as porcentaje_aperturadas,
        (cerradas       *100.00 / mesasxcentro) as porcentaje_cerradas,
        (transmitidas   *100.00 / mesasxcentro) as porcentaje_transmitidas

  from sde.cne_mesas;

  ---------------------------------------------  a proceso de porcentaje

UPDATE sde.cne_proceso
SET    porcentajemesasr         = cne_porcentaje.porcentaje_recibidas,
       porcenajemesasinstall    = cne_porcentaje.porcentaje_instaladas,
       porcconsti               = cne_porcentaje.porcentaje_constituidas,
       porcaperturadas          = cne_porcentaje.porcentaje_aperturadas,
       porccerradas             = cne_porcentaje.porcentaje_cerradas,
       porctransmitidas         = cne_porcentaje.porcentaje_transmitidas
FROM   sde.cne_porcentaje
WHERE  cne_proceso.codigocv = cne_porcentaje.centro;

--------------------------------------------- a cne_centros_alcaldes de proceso

UPDATE sde.centros_electorales_alcaldes
  SET
      mesastotales               = cne_proceso.mesas_totales, 
      mesasrecibidas             = cne_proceso.mesasrecibidas, 
      porcentaje_recibidas       = cne_proceso.porcentajemesasr, 
      mesasinstaladas            = cne_proceso.mesasinstaladas, 
      porcentaje_instaladas      = cne_proceso.porcenajemesasinstall, 
      mesasconstituidas          = cne_proceso.mesasconstituidas, 
      porcentaje_constituidas    = cne_proceso.porcconsti, 
      mesasaperturadas           = cne_proceso.mesasaperturadas, 
      porcentaje_aperturadas     = cne_proceso.porcaperturadas, 
      mesascerradas              = cne_proceso.mesascerradas, 
      porcentaje_cerradas        = cne_proceso.porccerradas, 
      mesastransmitidas          = cne_proceso.mesastransmitidas, 
      porcentaje_transmitidas    = cne_proceso.porctransmitidas
    
    FROM cne_proceso
     
  WHERE centros_electorales_alcaldes.codigo = cne_proceso.codigocv;
  
  -----------------------------------------------------------------
 ----------------------------------------
drop table if exists sde.cne_estado;
CREATE TABLE cne_estado as 
SELECT estado, 
    SUM (mesasrecibidas)      as mesasrecibidas,
    SUM (mesasinstaladas)     as mesasinstaladas,
    SUM (mesasconstituidas)   as mesasconstituidas,
    SUM (mesasaperturadas)    as mesasaperturadas,
    SUM (mesascerradas)       as mesascerradas,
    SUM (mesastransmitidas)   as mesastransmitidas,
    SUM (mesastotales)        as mesastotales,
    COUNT(codigo) AS centros
FROM sde.centros_electorales_alcaldes
group by estado
order by estado ASC;

--------------------------------------- Porcentaje estados
drop table if exists sde.cne_estado_porcentaje;
CREATE TABLE sde.cne_estado_porcentaje as 
SELECT estado, mesastotales,

        (mesasrecibidas      *100.00 / mesastotales) as porcentaje_recibidas,
        (mesasinstaladas     *100.00 / mesastotales) as porcentaje_instaladas,
        (mesasconstituidas   *100.00 / mesastotales) as porcentaje_constituidas,
        (mesasaperturadas    *100.00 / mesastotales) as porcentaje_aperturadas,
        (mesascerradas       *100.00 / mesastotales) as porcentaje_cerradas,
        (mesastransmitidas   *100.00 / mesastotales) as porcentaje_transmitidas,
       centros

  from sde.cne_estado;
-------------------------------------------------------
 
drop table if exists sde.cne_estado_total;
create table sde.cne_estado_total as
select  cne_estado.estado,
        cne_estado_porcentaje.mesastotales,
        cne_estado.mesasrecibidas,
        cne_estado_porcentaje.porcentaje_recibidas,
        cne_estado.mesasinstaladas,
        cne_estado_porcentaje.porcentaje_instaladas,
        cne_estado.mesasconstituidas,
        cne_estado_porcentaje.porcentaje_constituidas,
        cne_estado.mesasaperturadas,
        cne_estado_porcentaje.porcentaje_aperturadas,
        cne_estado.mesascerradas,
        cne_estado_porcentaje.porcentaje_cerradas,
        cne_estado.mesastransmitidas,
        cne_estado_porcentaje.porcentaje_transmitidas,
        cne_estado.centros
from cne_estado, cne_estado_porcentaje
where cne_estado.estado = cne_estado_porcentaje.estado;

--------------------------------------------------------
drop table if exists cne_redi;
CREATE TABLE cne_redi as
SELECT redi, 
    SUM (mesasrecibidas)      as mesasrecibidas,
    SUM (mesasinstaladas)     as mesasinstaladas,
    SUM (mesasconstituidas)   as mesasconstituidas,
    SUM (mesasaperturadas)    as mesasaperturadas,
    SUM (mesascerradas)       as mesascerradas,
    SUM (mesastransmitidas)   as mesastransmitidas,
    SUM (mesastotales)        as mesastotales,
    COUNT(codigo) AS centros
FROM sde.centros_electorales_alcaldes
group by redi
order by redi ASC;

--------------------------------------- Porcentaje Redis
drop table if exists sde.cne_redi_porcentaje;
CREATE TABLE sde.cne_redi_porcentaje as 
SELECT redi, mesastotales,

        (mesasrecibidas      *100.00 / mesastotales) as porcentaje_recibidas,
        (mesasinstaladas     *100.00 / mesastotales) as porcentaje_instaladas,
        (mesasconstituidas   *100.00 / mesastotales) as porcentaje_constituidas,
        (mesasaperturadas    *100.00 / mesastotales) as porcentaje_aperturadas,
        (mesascerradas       *100.00 / mesastotales) as porcentaje_cerradas,
        (mesastransmitidas   *100.00 / mesastotales) as porcentaje_transmitidas,
       centros
  from sde.cne_redi;
--------------------------------------------------------
drop table if exists sde.cne_redi_total;
create table sde.cne_redi_total as
select  cne_redi.redi,
        cne_redi_porcentaje.mesastotales,
        cne_redi.mesasrecibidas,
        cne_redi_porcentaje.porcentaje_recibidas,
        cne_redi.mesasinstaladas,
        cne_redi_porcentaje.porcentaje_instaladas,
        cne_redi.mesasconstituidas,
        cne_redi_porcentaje.porcentaje_constituidas,
        cne_redi.mesasaperturadas,
        cne_redi_porcentaje.porcentaje_aperturadas,
        cne_redi.mesascerradas,
        cne_redi_porcentaje.porcentaje_cerradas,
        cne_redi.mesastransmitidas,
        cne_redi_porcentaje.porcentaje_transmitidas,
        cne_redi.centros
from cne_redi, cne_redi_porcentaje
where cne_redi.redi = cne_redi_porcentaje.redi;
--------------------------------------------------------
drop table if exists cne_zodi;
CREATE TABLE cne_zodi as
SELECT zodi, cod_zod, 
    SUM (mesasrecibidas)      as mesasrecibidas,
    SUM (mesasinstaladas)     as mesasinstaladas,
    SUM (mesasconstituidas)   as mesasconstituidas,
    SUM (mesasaperturadas)    as mesasaperturadas,
    SUM (mesascerradas)       as mesascerradas,
    SUM (mesastransmitidas)   as mesastransmitidas,
    SUM (mesastotales)        as mesastotales,
    COUNT(codigo) AS centros
FROM sde.centros_electorales_alcaldes
group by zodi,cod_zod
order by cod_zod ASC;

-------------------------------------------------
drop table if exists sde.cne_zodi_porcentaje;
CREATE TABLE sde.cne_zodi_porcentaje as 
SELECT zodi, cod_zod, mesastotales,

        (mesasrecibidas      *100.00 / mesastotales) as porcentaje_recibidas,
        (mesasinstaladas     *100.00 / mesastotales) as porcentaje_instaladas,
        (mesasconstituidas   *100.00 / mesastotales) as porcentaje_constituidas,
        (mesasaperturadas    *100.00 / mesastotales) as porcentaje_aperturadas,
        (mesascerradas       *100.00 / mesastotales) as porcentaje_cerradas,
        (mesastransmitidas   *100.00 / mesastotales) as porcentaje_transmitidas,
       centros
  from sde.cne_zodi;
-----------------------------------   
drop table if exists cne_zodi_total;
create table cne_zodi_total as
select  cne_zodi.zodi,
        cne_zodi_porcentaje.mesastotales,
        cne_zodi.mesasrecibidas,
        cne_zodi_porcentaje.porcentaje_recibidas,
        cne_zodi.mesasinstaladas,
        cne_zodi_porcentaje.porcentaje_instaladas,
        cne_zodi.mesasconstituidas,
        cne_zodi_porcentaje.porcentaje_constituidas,
        cne_zodi.mesasaperturadas,
        cne_zodi_porcentaje.porcentaje_aperturadas,
        cne_zodi.mesascerradas,
        cne_zodi_porcentaje.porcentaje_cerradas,
        cne_zodi.mesastransmitidas,
        cne_zodi_porcentaje.porcentaje_transmitidas,
        cne_zodi.centros
from cne_zodi, cne_zodi_porcentaje
where cne_zodi.zodi = cne_zodi_porcentaje.zodi;
--------------------------------------------------- 
drop table if exists cne_estadistica_general;
CREATE TABLE cne_estadistica_general as
SELECT  
    SUM (mesasrecibidas)      as mesasrecibidas,
    SUM (mesasinstaladas)     as mesasinstaladas,
    SUM (mesasconstituidas)   as mesasconstituidas,
    SUM (mesasaperturadas)    as mesasaperturadas,
    SUM (mesascerradas)       as mesascerradas,
    SUM (mesastransmitidas)   as mesastransmitidas,
    SUM (mesastotales)        as mesastotales,
    COUNT(codigo) AS centros
FROM sde.centros_electorales_alcaldes;

--------------------------------------- Porcentaje Estadisticas Generales
drop table if exists sde.cne_general_porcentaje;
CREATE TABLE sde.cne_general_porcentaje as 
SELECT 
         (mesasrecibidas      *100.00 / mesastotales) as porcentaje_recibidas,
         (mesasinstaladas     *100.00 / mesastotales) as porcentaje_instaladas,
         (mesasconstituidas   *100.00 / mesastotales) as porcentaje_constituidas,
         (mesasaperturadas    *100.00 / mesastotales) as porcentaje_aperturadas,
         (mesascerradas       *100.00 / mesastotales) as porcentaje_cerradas,
         (mesastransmitidas   *100.00 / mesastotales) as porcentaje_transmitidas,
       centros
  from sde.cne_estadistica_general;
------------------------------------- Estadisticas Generales
drop table if exists cne_esge_total;
create table cne_esge_total as
select  
        cne_estadistica_general.mesastotales,
        cne_estadistica_general.mesasrecibidas,
        cne_general_porcentaje.porcentaje_recibidas,
        cne_estadistica_general.mesasinstaladas,
        cne_general_porcentaje.porcentaje_instaladas,
        cne_estadistica_general.mesasconstituidas,
        cne_general_porcentaje.porcentaje_constituidas,
        cne_estadistica_general.mesasaperturadas,
        cne_general_porcentaje.porcentaje_aperturadas,
        cne_estadistica_general.mesascerradas,
        cne_general_porcentaje.porcentaje_cerradas,
        cne_estadistica_general.mesastransmitidas,
        cne_general_porcentaje.porcentaje_transmitidas,
        cne_estadistica_general.centros
from cne_estadistica_general, cne_general_porcentaje; 
----------------------------------------- tabla porcentajes totales de los CV
ALTER TABLE cne_esge_total ADD COLUMN gfh TIMESTAMP;
update cne_esge_total 
set gfh = current_timestamp;

ALTER TABLE cne_esge_total
    ALTER COLUMN gfh TYPE varchar,
    ALTER COLUMN gfh SET NOT NULL;
---------------------------------------- tabla con el resultado final de mesas
drop table if exists cne_totales;
create table cne_totales as 
select 
        (mesastotales -  mesasrecibidas)    as mesasrecibidas,
        (mesastotales -  mesasinstaladas)   as mesasinstaladas,
        (mesastotales -  mesasconstituidas) as mesasconstituidas,
        (mesastotales -  mesasaperturadas)  as mesasaperturadas,
        (mesastotales -  mesascerradas)     as mesascerradas,
        (mesastotales -  mesastransmitidas) as mesastransmitidas
from sde.cne_estadistica_general, sde.cne_general_porcentaje;
 
----------------------------------------- tabla porcentajes totales de los CV
ALTER TABLE cne_esge_total ADD COLUMN gfh TIMESTAMP;
update cne_esge_total
set gfh = current_timestamp;

ALTER TABLE cne_esge_total
    ALTER COLUMN gfh TYPE varchar,
    ALTER COLUMN gfh SET NOT NULL;

----------------------------------------
SET timezone TO 'America/Caracas';
UPDATE cne_gfh_fc
set gfh = NOW();

select * from cne_gfh_fc
----------------------------------------- tabla porcentajes totales de los CV

  select * from cne_esge_total;
