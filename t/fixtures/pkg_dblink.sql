CREATE OR REPLACE EDITIONABLE PACKAGE APP."DEMO_PKG"
AS
   PROCEDURE load_items (p_from DATE, p_to DATE);
   FUNCTION check_one (p_ref VARCHAR2) RETURN VARCHAR2;
   PROCEDURE fetch_attrs (p_group VARCHAR2, p_class VARCHAR2);
   FUNCTION НАЙТИ_ТОВАР (p_ident NUMBER) RETURN NUMBER;
END DEMO_PKG;
/
CREATE OR REPLACE EDITIONABLE PACKAGE BODY APP."DEMO_PKG"
AS
   PROCEDURE load_items (p_from DATE, p_to DATE)
   IS
      new_item_id   NUMBER;
      l_request     rsch.ext_checks.verify_id@rdb1%TYPE;
      l_calc      NUMBER;
   BEGIN
      SELECT rsch.seq_item_s.NEXTVAL@rdb1
        INTO new_item_id
        FROM DUAL;

      l_request := rsch.rem_iface_pkg.ref_id@rdb1;

      -- rename groups from the remote dictionary
      UPDATE ГРУППА_ТОВАРОВ st
         SET st.название = (SELECT v.key_name FROM rsch.rem_accounts@rdb1 v WHERE v.key_code = to_char(st.группа))
       WHERE st.группа IN (SELECT v.key_code FROM rsch.rem_accounts@rdb1 v);

      rsch.rem_calc_pkg.get_calc_data@rdb1 (p_from, 'A', 'B', l_calc, l_calc, l_calc, l_calc);

      UPDATE EXTSCH.IMPORT_TBL@ext1 d
         SET DIR_CONDITION = 0
       WHERE d.id = new_item_id;

      INSERT INTO rsch.item_log@rdb1 (id, dt) VALUES (new_item_id, SYSDATE);
   END load_items;

   FUNCTION check_one (p_ref VARCHAR2) RETURN VARCHAR2
   IS
      l_state   VARCHAR2 (100);
   BEGIN
      l_state :=
         rsch.rem_iface_pkg.verify_state@rdb1 (
            p_ref   => p_ref);
      RETURN l_state;
   END check_one;

   PROCEDURE fetch_attrs (p_group VARCHAR2, p_class VARCHAR2)
   IS
      x_comb   NUMBER;
      x_cat    NUMBER;
      err_text  VARCHAR2 (4000);
   BEGIN
      rsch.rem_iface_pkg.get_item_info@rdb1 (
         p_dept                   => p_group,
         p_kind                => p_group,
         x_combo_id   => x_comb,
         x_cat_id           => x_cat,
         X_ERR_TEXT               => err_text,
         p_doc_type              => p_class);
      rsch.rem_iface_pkg.add_grp_code@rdb1 (p_group);
   END fetch_attrs;

   FUNCTION НАЙТИ_ТОВАР (p_ident NUMBER)
      RETURN NUMBER
   IS
      res   NUMBER DEFAULT NULL;
   BEGIN
      SELECT MIN (op.ТОВАР)
        INTO res
        FROM ТОВАР op, ТОВАР op2
       WHERE op.партия = op2.партия
         AND op2.товар = p_ident;

      RETURN res;
   END;
END DEMO_PKG;
/
