           -- 2.1. Изменение наименований статей
           UPDATE ГРУППА_ТОВАРОВ st
              SET st.название = (SELECT v.key_name FROM rsch.rem_accounts@rdb1 v where v.key_code = to_char(st.группа))
            where st.группа in
              (
               select v.key_code
               from rsch.rem_accounts@rdb1 v where nvl(v.key_name,'*****') <> nvl(st.название,'*****')
              );
