-- Five ad-hoc FDW scans per pgbench transaction.  The predicates deliberately
-- return no rows: this exercises connection cleanup without moving useful
-- data or making the SQL Anywhere fixture do significant work.
BEGIN;
SELECT codi_emp FROM dominio.geempre_slow WHERE codi_emp = -2147483648;
SELECT codi_emp FROM dominio.efentradas_slow WHERE codi_emp = -2147483648;
SELECT codi_emp FROM dominio.efsaidas WHERE codi_emp = -2147483648;
SELECT codi_emp FROM dominio.foempregados_slow WHERE codi_emp = -2147483648;
SELECT codi_emp FROM dominio.focargos_slow WHERE codi_emp = -2147483648;
COMMIT;
