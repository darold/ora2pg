--
-- V_SALES_TOTAL  (View)
--
CREATE OR REPLACE FORCE EDITIONABLE VIEW APP.V_SALES_TOTAL
(SM, DFROM)
BEQUEATH DEFINER
AS
select /*+ ordered use_nl(tp)*/sum(decode(d.k_vf,
                       1, d.unit_price,
                       d.unit_price/(sw.rate@dbb_bak(d.k_vf, d.dfrom,1))
                       ))  Sm, d.dfrom from sw.doctype@dbb_bak tp,sw.doc@dbb_bak d
                       where upper(tp.fullpathnm) like '%ПРОДАЖ%ИТОГ%'
                         and sw.rate@dbb_bak(d.k_vf, d.dfrom,1)<>0
                         and d.tp=tp.k_c
    Group By d.dfrom;
