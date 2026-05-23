-- Cria o usuário/schema 'bethadba' (espelhando produção). O usuário 'externo'
-- já foi criado pelo dbinit como DBA — agora damos a ele acesso aos objetos
-- de 'bethadba' via grupo.
--
-- Em SQL Anywhere, "schema" = "user". Os FOREIGN TABLE options apontam para
-- "bethadba.efsaidas", e o FDW filtra sys.systable por u.user_name='bethadba'.

CREATE USER bethadba IDENTIFIED BY 'bethadba_pwd_123';
GRANT DBA TO bethadba;
